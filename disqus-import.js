const MongoClient = require('mongodb').MongoClient
const fs = require('fs')
const XmlStream = require('xml-stream')
const moment = require('moment')
const config = require('./config')

const MONGODB_PORT = config.mongodb_port
const MONGODB_HOST = config.mongodb_host
const MONGODB_DB_NAME = config.mongodb_db_name
const PATH_TO_DISQUS_COMMENTS_XML = config.path_to_disqus_comments_xml
const COLLECTION_DISQUS_THREADS = config.collection_disqus_threads
const COLLECTION_DISQUS_POSTS = config.collection_disqus_posts
const MAX_CHUNK_SIZE = config.max_chunk_size
const THREAD_LINKS_FILTER = config.thread_links_filter || []
const url = `mongodb://${MONGODB_HOST}:${MONGODB_PORT}`

const client = new MongoClient(url, { useUnifiedTopology: true })

function importDisqusItems(db, collectionName, nodeName, transformFn) {
  return new Promise((resolve, reject) => {
    console.log(`Importing into temporary collection ${collectionName} Disqus ${nodeName}...`)
    let start = new Date().getTime()
    const stream = fs.createReadStream(PATH_TO_DISQUS_COMMENTS_XML)
    const xml = new XmlStream(stream, 'utf8')
    let readCounter = 0
    let importedCounter = 0
    let bulkChunks = 0
    let chunksBuffer = []
    xml.on(`endElement: ${nodeName}`, async function (node) {
      const { skip, transformed } = transformFn(node)
      if (skip) {
        return
      }
      chunksBuffer.push(transformed)
      if (chunksBuffer.length === MAX_CHUNK_SIZE) {
        await persistRecord({ db, collectionName, xml, chunksBuffer })
        importedCounter += MAX_CHUNK_SIZE
        bulkChunks++
        console.log('imported chunks: ' + bulkChunks)
      }
      readCounter++
    })
    xml.on('end', async function () {
      const length = chunksBuffer.length
      await persistRecord({ db, collectionName, xml, chunksBuffer })
      importedCounter += length
      bulkChunks++
      console.log('imported chunks: ' + bulkChunks)
      console.log(`Disqus ${nodeName} were successfully imported. Imported ${importedCounter} of ${readCounter}. Time spent ms: ${new Date().getTime() - start}`)
      return resolve()
    })
    xml.on('error', function (message) {
      console.error('Parsing failed: ' + message)
      return reject()
    })
  })
}

function transformThreadNode(node) {
  if (!node.category) {
    return { skip: true } // It's thread node inside post, skip it
  }
  const transformed = {
    ...node,
    _id: node['$']['dsq:id'],
    isClosed: toBoolean(node.isClosed),
    isDeleted: toBoolean(node.isDeleted),
    createdAt: moment.utc(node.createdAt).toDate()
  }
  if (transformed.author) {
    transformed.author = {
      ...transformed.author,
      isAnonymous: toBoolean(transformed.author.isAnonymous)
    }
  }
  delete transformed['$']
  delete transformed.category
  return { skip: false, transformed }
}

function transformPostNode(node) {
  const transformed = {
    ...node,
    _id: node['$']['dsq:id'],
    thread: node.thread['$']['dsq:id'],
    parent: node.parent ? node.parent['$']['dsq:id'] : null,
    isSpam: toBoolean(node.isSpam),
    isDeleted: toBoolean(node.isDeleted),
    createdAt: moment.utc(node.createdAt).toDate()
  }
  if (transformed.author) {
    transformed.author = {
      ...transformed.author,
      isAnonymous: toBoolean(transformed.author.isAnonymous)
    }
  }
  delete transformed['$']
  return { skip: false, transformed }
}

async function persistRecord({ db, collectionName, xml, chunksBuffer }) {
  if (chunksBuffer.length === 0) {
    return
  }
  if (!xml._suspended) {
    xml.pause()
  }
  try {
    await db.collection(collectionName)
      .bulkWrite(
        chunksBuffer.map(doc => (
            { updateOne: { filter: { _id: doc._id }, update: doc, upsert: true } }
          )
        )
      )
    chunksBuffer.splice(0, chunksBuffer.length)
  } catch (err) {
    console.error('Can not insert bunch of records into mongo', chunksBuffer, err)
  } finally {
    if (xml._suspended) {
      xml.resume()
    }
  }
}

function toBoolean(strBoolean) {
  return ('' + strBoolean).toLowerCase() === 'true'
}

async function createTempCollections(db) {
  if (await isCollectionExist(db, COLLECTION_DISQUS_THREADS)) {
    await db.dropCollection(COLLECTION_DISQUS_THREADS)
  }
  if (await isCollectionExist(db, COLLECTION_DISQUS_POSTS)) {
    await db.dropCollection(COLLECTION_DISQUS_POSTS)
  }
  await db.createCollection(COLLECTION_DISQUS_THREADS)
  await db.createCollection(COLLECTION_DISQUS_POSTS)
}

async function isCollectionExist(db, collectionName) {
  const collections = await db.listCollections().toArray()
  return collections.map(c => c.name).includes(collectionName)
}

async function findReplies(db, threadId, parentPostId) {
  const condition = {
    $and: [
      {
        thread: threadId
      },
    ]
  }
  if (parentPostId === null) {
    condition.$and.push(
      { $or: [{ parent: { $exists: true, $eq: null } }, { parent: { $exists: false } }] }
    )
  } else {
    condition.$and.push(
      { parent: { $exists: true, $eq: parentPostId } }
    )
  }
  const countOfReplies = await db.collection(COLLECTION_DISQUS_POSTS)
    .find(condition)
    .count()
  if (countOfReplies === 0) {
    return []
  }
  let replies = []
  const postsCursor = await db.collection(COLLECTION_DISQUS_POSTS)
    .find(condition)
    .sort({ createdAt: -1 })
  for await(const post of postsCursor) {
    if (post.isSpam || post.isDeleted) {
      continue
    }
    post.replies = [...await findReplies(db, threadId, post._id)]
    replies.push(post)
  }
  return replies
}

async function convertToCommentsUiComments(db) {
  console.log('Converting Disqus threads/posts to comments-ui...')
  const start = new Date().getTime()
  const condition = {}
  if (config.thread_links_regex && config.thread_links_regex.length > 0) {
    condition.$or = [
      ...config.thread_links_regex.map(regExpr => (
        { link: { $regex: new RegExp(regExpr) } })
      )
    ]
  }
  const threads = db.collection(COLLECTION_DISQUS_THREADS)
    .find(condition)
    .sort({ createdAt: -1 })
  for await(const thread of threads) {
    if (thread.isSpam || thread.isDeleted) {
      continue
    }
    thread.replies = [
      ...(await findReplies(db, thread._id, null)) || []
    ]
    console.log(JSON.stringify(thread) + ',')
  }
  console.log(`Converting Disqus to comments-ui finished. Time spent ms: ${new Date().getTime() - start}`)
}

async function startImport() {
  try {
    await client.connect()
    console.log("Connected successfully to server")
    const db = client.db(MONGODB_DB_NAME)
    // await createTempCollections(db)
    // await importDisqusItems(db, COLLECTION_DISQUS_THREADS, 'thread', transformThreadNode)
    // await importDisqusItems(db, COLLECTION_DISQUS_POSTS, 'post', transformPostNode)
    await convertToCommentsUiComments(db)
    // const replies = await findReplies(db, "7511555903", null)
    // console.log(JSON.stringify(replies))
  } catch (err) {
    console.error(err)
  } finally {
    client.close().catch((err) => console.error(err))
  }
}

startImport()
