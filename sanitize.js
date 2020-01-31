const sanitizeHtml = require('sanitize-html')

const sanitizeRichTextOptions = {
  ...sanitizeHtml.defaults,
  allowedTags: [
    'p',
    'br',
  ],
  allowedAttributes: {},
}

function htmlToText(value) {
  let text =  value ? sanitizeHtml(value, sanitizeRichTextOptions) : value
  return text
    .replace(/<p?\s*>/g, '')
    .replace(/<\/p\s*?>/g, '\n')
    .replace(/<br\s*\/?>/g, '\n')
    .trim()
}

module.exports = htmlToText


