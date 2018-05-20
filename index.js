'use strict'

// require('dotenv').config()
// const {} = process.env
const serverless = require('serverless-http')
const AWS = require('aws-sdk')
const Koa = require('koa')
const body = require('koa-body')
const Boom = require('boom')
const Router = require('koa-router')
const uuidv1 = require('uuid/v1')

const INITIAL_UPDATE_ID = 'initial'

const app = new Koa()
module.exports.handler = serverless(app)
const ddbService = new AWS.DynamoDB({
  apiVersion: '2012-08-10',
  region: 'eu-west-1',
  output: 'json'
})
const ddb = new AWS.DynamoDB.DocumentClient({
  service: ddbService
})
const TableName = 'MW_MatchUpdates'

const router = Router()

router.get('/match', async (ctx, next) => {
  const list = await listAllMatches()
  ctx.response.body = list
  return next()
})
router.post('/match', body(), async (ctx, next) => {
  const matchId = uuidv1()
  await updateMatch(matchId, ctx.request.body)
  ctx.status = 200
  ctx.body = {id: matchId}
  return next()
})
router.get('/match/last', async (ctx, next) => {
  const match = await getLastMatch()
  ctx.response.body = match
  return next()
})
router.patch('/match/last', body(), async (ctx, next) => {
  const {id: matchId} = await getLastMatch()
  const matchData = ctx.request.body
  await updateMatch(matchId, matchData)
  ctx.status = 200
  ctx.body = {}
  return next()
})
// router.get('/match/:matchId', getMatchRequest)
router.patch('/match/:matchId', body(), async (ctx, next) => {
  const matchId = ctx.params.matchId
  const matchData = ctx.request.body
  await updateMatch(matchId, matchData)
  ctx.status = 200
  return next()
})

app.use(router.routes())
app.use(router.allowedMethods({
  throw: true,
  notImplemented: () => Boom.notImplemented(),
  methodNotAllowed: () => Boom.methodNotAllowed()
}))

if (!module.parent) app.listen(1337)

async function listAllMatches () {
  try {
    const params = {
      TableName,
      FilterExpression: 'updateId = :updateId',
      ExpressionAttributeValues: {
        ':updateId': INITIAL_UPDATE_ID
      }
    }

    const result = await ddb.scan(params).promise()
    return result
  } catch (ex) {
    console.error(`ERROR in 'listAllMatches': `, ex)
  }
}
/*
async function createMatch (matchData) {
  const id = uuidv1()
  try {
    const Item = {
      id,
      updateId: INITIAL_UPDATE_ID,
      timestamp: Date.now(),
      ...matchData
    }
    const params = {
      TableName,
      Item,
      Expected: {
        id: {
          Exists: false
        }
      }
    }
    await ddb.put(params).promise()
    return await updateMatch(id, matchData)
  } catch (ex) {
    if (ex.code === 'ConditionalCheckFailedException') {
      console.info(`item already exists with id=${id}`)
    } else {
      console.error(`ERROR in 'createMatch': `, ex)
    }
  }
}
*/
async function updateMatch (matchId, matchData) {
  try {
    const Item = {
      id: matchId,
      updateId: INITIAL_UPDATE_ID,
      timestamp: Date.now(),
      ...matchData
    }
    const params = {
      TableName,
      Item
    }
    await ddb.put(params).promise()
    params.Item.updateId = uuidv1()
    params.Expected = {
      id: {
        Exists: false
      }
    }
    await ddb.put(params).promise()
  } catch (ex) {
    console.error(`ERROR in 'updateMatch': `, ex)
  }
}

async function getLastMatch () {
  try {
    const {Items: matches} = await listAllMatches()
    matches.sort((lhs, rhs) => rhs.timestamp - lhs.timestamp)
    console.log(matches[0])
    return matches[0]
  } catch (ex) {
    console.error(`ERROR in 'getLastMatchId': `, ex)
  }
}
/*
async function getMatch (matchId) {
  try {
    const params = {
      TableName,
      KeyConditionExpression: 'id = :id',
      ExpressionAttributeValues: {
        ':id': matchId
      }
    }
    const result = await ddb.query(params).promise()
    return result
  } catch (ex) {
    console.error(`ERROR in 'getMatch': `, ex)
  }
}
*/

/*
aws --endpoint-url http://localhost:8000 dynamodb create-table --table-name MW_MatchUpdates \
 --attribute-definitions AttributeName=id,AttributeType=S AttributeName=updateId,AttributeType=S \
 --key-schema AttributeName=id,KeyType=HASH AttributeName=updateId,KeyType=RANGE \
 --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=5

aws --endpoint-url http://localhost:8000 dynamodb delete-table --table-name MW_MatchUpdates
*/
