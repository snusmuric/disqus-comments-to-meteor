# disqus-comments-to-meteor
## Imports Disqus comments (XML export) to Mongodb. 

In Mongo, comments are stored in the format described here: https://github.com/komentify/meteor-comments-ui

### Setup
Copy `config_example.json` to `config.json` and modify it in order to specify connection parameters to your Mongo
and path to Disqus xml export file.

### How to run
```
npm install

node disqus-import.js --config=path_to_your_config.json
```


