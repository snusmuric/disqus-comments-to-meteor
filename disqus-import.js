const MongoClient = require('mongodb').MongoClient
const fs = require('fs')
const XmlStream = require('xml-stream')
const moment = require('moment')
const config = require('./config')
const htmlToText = require('./sanitize')

const MONGODB_PORT = config.mongodb_port
const MONGODB_HOST = config.mongodb_host
const MONGODB_DB_NAME = config.mongodb_db_name
const PATH_TO_DISQUS_COMMENTS_XML = config.path_to_disqus_comments_xml
const COLLECTION_DISQUS_THREADS = config.collection_disqus_threads
const COLLECTION_DISQUS_POSTS = config.collection_disqus_posts
const COLLECTION_COMMENTS = config.collection_comments_ui_comments
const MAX_CHUNK_SIZE = config.max_chunk_size
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
        await persistDisqusItem({ db, collectionName, xml, chunksBuffer })
        importedCounter += MAX_CHUNK_SIZE
        bulkChunks++
        console.log('imported chunks: ' + bulkChunks)
      }
      readCounter++
    })
    xml.on('end', async function () {
      const length = chunksBuffer.length
      await persistDisqusItem({ db, collectionName, xml, chunksBuffer })
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

async function persistDisqusItem({ db, collectionName, xml, chunksBuffer }) {
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

function referenceIdFromUrl(url) {
  if (!url) {
    return null
  }
  const urlTruncated = url.replace(/\/$/, '')
  const searchParams = Array.from(new URLSearchParams(urlTruncated).values())
  if (searchParams && searchParams.length > 0 && searchParams[0] !== '') {
    return searchParams[0]
  }
  const pathParts = new URL(urlTruncated).pathname
    .replace(/\/$/, '')
    .split('/')
  const pathParam = pathParts && pathParts.length > 0 ? pathParts[pathParts.length - 1] : null
  return pathParam !== '' ? pathParam : null
}

function normalizePostMessage(message) {
  return htmlToText(message)
}

async function findUserIdByName(db, name) {
  if (!name) {
    return null
  }
  let userId = await db.collection('users').findOne({ name })
  if (!userId) {
    const caseInsensitiveRegex = new RegExp(`^${name.toLowerCase()}$i`)
    userId = await db.collection('users').findOne({ name: caseInsensitiveRegex })
  }
  return userId ? userId._id : null
}

async function toComment(db, disqusData, post) {
  const comment = {
    referenceId: disqusData.postId,
    fromDisqus: true,
    disqus: {
      ...disqusData,
      author: post.author,
      parent: post.parent,
      thread: post.thread,
    },
    content: normalizePostMessage(post.message),
    userId: !post.author.isAnonymous
      ? (await findUserIdByName(db, post.author.name))
      : null,
    isAnonymous: post.author.isAnonymous || false,
    createdAt: post.createdAt,
    likes: [],
    dislikes: [],
    replies: [],
    media: {},
    status: 'approved',
    starRatings: [],
    ratingScore: 0,
    lastUpdatedAt: post.createdAt,
  }
  if (post.parent === null ) {
    comment._id = `disqus_${post._id}`
  } else {
    comment.replyId = `disqus_${post._id}`
  }
  return comment
}

async function findReplies(db, threadId, disqusData, parentPostId) {
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
    const comment = await toComment(db, disqusData, post)
    comment.replies = [...await findReplies(db, threadId, disqusData, post._id)]
    replies.push(comment)
  }
  return replies
}

async function persistComments(db, replies) {
  try {
    await db.collection(COLLECTION_COMMENTS)
      .bulkWrite(
        replies.map(doc => (
            { updateOne: { filter: { _id: doc._id }, update: doc, upsert: true } }
          )
        )
      )
  } catch (err) {
    console.error('Can not insert bunch of comments into mongo', replies, err)
  }
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
  const countOfThreads = await threads.count()
  console.log(`Filtered ${countOfThreads} threads`)
  let convertedThreads = 0
  for await(const thread of threads) {
    if (thread.isSpam || thread.isDeleted) {
      continue
    }
    const disqusData = {
      id: thread._id,
      link: thread.link,
      postId: referenceIdFromUrl(thread.link),
    }
    const replies = [
      ...(await findReplies(db, thread._id, disqusData, null)) || []
    ]
    if (replies.length === 0) {
      console.log(`Thread ${thread.link} is skipped, it has zero comments`)
      continue
    }
    convertedThreads++
    console.log(JSON.stringify(replies) + ',')
    await persistComments(db, replies)
  }
  console.log(`Converting Disqus to comments-ui finished. Converted ${convertedThreads} threads. Time spent ms: ${new Date().getTime() - start}`)
}

async function startImport() {
  try {
    await client.connect()
    console.log("Connected successfully to server")
    const db = client.db(MONGODB_DB_NAME)
    await createTempCollections(db)
    await importDisqusItems(db, COLLECTION_DISQUS_THREADS, 'thread', transformThreadNode)
    await importDisqusItems(db, COLLECTION_DISQUS_POSTS, 'post', transformPostNode)
    await convertToCommentsUiComments(db)
  } catch (err) {
    console.error(err)
  } finally {
    client.close().catch((err) => console.error(err))
  }
}

startImport()
