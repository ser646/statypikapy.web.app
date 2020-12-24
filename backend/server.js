var uri = process.env.mongodb2;
var http = require('http');
var url = require('url');

var app_port = process.env.app_port || 8081;
var app_host = process.env.app_host || '192.168.0.2';

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const fetch = require('node-fetch');
var SteamID = require('steamid');
const { SSL_OP_SSLEAY_080_CLIENT_DH_BUG } = require('constants');
const { Z_ASCII } = require('zlib');
const { timeStamp } = require('console');

function chunk(array, size) {
    const chunked_arr = [];
    let index = 0;
    while (index < array.length) {
      chunked_arr.push(array.slice(index, size + index));
      index += size;
    }
    return chunked_arr;
  }

  function startOfWeek(date){
    d = new Date(date);
    var day = d.getDay(),
    diff = d.getDate() - day + (day == 0 ? -6:1); // adjust when day is sunday
		d.setDate(diff)
		d.setHours(0)
		d.setMinutes(0)
		d.setSeconds(0)
		d.setMilliseconds(0)
    return new Date(d);
}
function startOfMonth(date){
    date = new Date(date);
    return new Date(date.getFullYear(), date.getMonth(), 1);
}

http.createServer(function(req, res) {
    const queryObject = url.parse(req.url,true).query; 
    res.setHeader("Access-Control-Allow-Origin", "*");
    MongoClient.connect(uri, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }, function(err, client) {
        assert.equal(null, err);
        res.setHeader('Content-Type', 'application/json');
        const db = client.db();

        async function getSteamProfiles(steamids64){
          p = [];
          steam_api_key = process.env.steam_api_key;
          date = Date.now();
          d = new Date(date)
          old_date = d.setDate(d.getMonth() - 1);
          
          
          already_in_db =  await db.collection('steam_profiles').find({
            'update_timestamp': {$gt : old_date},
            '_id': { $in: steamids64}
          }).toArray();
          db_ids = [];
          for(e of already_in_db)db_ids.push(e._id) 
          ids = steamids64.filter(x => !db_ids.includes(x));
          ids = chunk(ids, 100)
          
          for(i = 0;i < ids.length;i++){
              p[i] = fetch(`http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=${steam_api_key}&steamids=`+JSON.stringify(ids[i]))
                  .then(d => d.json())
                  .then(d => {
                    profiles = [];
                    d = d.response.players
                    for(p of d){
                      profiles.push({
                        _id : p.steamid,
                        avatar : p.avatar,
                        profileurl : p.profileurl,
                        personaname: p.personaname
                      })
                    }
                    return profiles;
                  })
                  .catch(e => {
                      console.log(e)
                  })
          }
          return await Promise.all(p).then(d => {
              merged = [];
              for(dd of d) {
                merged = merged.concat(dd)
              }
              for(m of merged){
                m['update_timestamp'] = date;
                db.collection('steam_profiles').replaceOne({'_id' : m._id},m,{upsert : true})
                  .then(_ => {console.log('Success');})
                  .catch(e => {console.log('Failure: '+e);})
              }
              for(dd of already_in_db) {
                merged = merged.concat(dd)
              }
              return merged;          
          })
        }

        if(queryObject['f'] == 0){
            db.collection('logs').distinct('_id', {}, {}, function (err, result) {
                for(i=result.length;i>result.length-queryObject['n'];i--){
                    db.collection('logs').deleteOne({_id : result[i]}, function(err, obj) {
                        if (err) throw err;
                    });
                }
                p2 = db.collection('logs').distinct('_id', {}, {}).then(r => {
                console.log('Deleted: '+queryObject['n']+' rows. Remaining: '+r.length)
                res.write('Deleted: '+queryObject['n']+' rows. Remaining: '+r.length);
                res.end();
                client.close();
                })
                
            })
        }
        else if(queryObject['f'] == 1){
            p1 = fetch("https://logs.tf/api/v1/log?title=tf2pickup.pl&limit=10000")
            .then(r => r.json()).then(r => {
                logs = [];
                for(l of r.logs){
                    logs.push(`${l.id}`)
                }
                return logs;
            })  
            
            p2 = db.collection('logs').distinct('_id', {}, {})
            //p2 = db.collection('logs').find({}, {_id:1}).map(function(item){ return item._id; }).toArray()

            Promise.all([p1,p2]).then(r => {
                let diff = r[0].filter(x => !r[1].includes(x));
                res.end(JSON.stringify(diff));
                client.close();
            })
        }
        else if(queryObject['f'] == 2){
            matchid = queryObject['matchid']
            fetch('http://logs.tf/api/v1/log/'+matchid).then(r => r.json()).then(r => {   
                r['_id'] = matchid;
                r['players_count'] = Object.keys(r.players).length;
                db.collection('logs').insertOne(r)
                .then(_ => {
                    res.write('Success');
                    res.end();
                    client.close();
                }).catch(e => {
                    res.write('Failure: '+e);
                    res.end();
                    client.close();
                })
            }).catch(e => {
                res.write('Failure: '+e);
                res.end();
                client.close();
            })
        }
        else if(queryObject['f'] == 3){
            time_range = queryObject['time_range']
            dt = new Date();
            if(time_range == 'all')dt = 1;
            else if(time_range == 'weekly')dt =  startOfWeek(dt);
            else if(time_range == 'monthly')dt =  startOfMonth(dt);
            z = new Date(dt);
            z = z.getTime()

            db.collection('logs').aggregate([
                {
                    '$match': {
                        'players_count': {
                            '$lt': 15
                        },
                        'info.date': {
                            '$gt': z/1000
                        }
                    }
                },
                {
                    '$sort' :{
                        'info.date': -1
                      }
                },
                {
                  '$project': {
                    '_id': 1, 
                    'map': '$info.map', 
                    'date': '$info.date', 
                    'result': {
                        'Red': '$teams.Red.score', 
                        'Blue': '$teams.Blue.score'
                    }
                  }
                }

              ])
            .toArray((err,r) => {
                res.end(JSON.stringify(r));
                client.close();
            })
        }
        else if(queryObject['f'] == 4){
            time_range = queryObject['time_range']
            dt = new Date();
            if(time_range == 'all')dt = 1;
            else if(time_range == 'weekly')dt =  startOfWeek(dt);
            else if(time_range == 'monthly')dt =  startOfMonth(dt);
            z = new Date(dt);
            z = z.getTime()

            players = {};
            steamids = [];
            steamids64 = [];
                      
            p2 = db.collection('logs').aggregate([
              {
                '$match': {
                  'info.date': {
                    '$gt': z / 1000
                  }, 
                  'players_count': {
                    '$lt': 15
                  }
                }
              }, {
                '$project': {
                  'players': {
                    '$map': {
                      'input': {
                        '$objectToArray': '$players'
                      }, 
                      'as': 'p', 
                      'in': {'id' : '$$p.k','team' : '$$p.v.team'}
                    }
                  }, 
                  'score': {
                    'Blue': '$teams.Blue.score', 
                    'Red': '$teams.Red.score'
                  },
                  'names' : 1
                }
              }
            ]).toArray(function (err,result){
              for(log of result){
                if(log.score.Red > log.score.Blue)winner = 'Red';
                else if(log.score.Red < log.score.Blue)winner = 'Blue';
                else winner = false;

                for(p of log.players){
                  id = p.id;
                  if(players[id] == undefined){
                      var sid = new SteamID(id);
                      steamids.push(id);
                      sid = sid.getSteamID64(); 
                      steamids64.push(sid);
                      players[id] = {
                          'name' : log.names[id],
                          'steamid64' : sid,
                          'avatar' : '',
                          'games_played' : 0,
                          'games_won' : 0,
                          'games_lost' : 0,
                          'games_tied' : 0,
                          'score' : 0
                      }
                  }
                  players[id].games_played += 1;
                  if(winner && p.team == winner){
                      players[id].games_won += 1;
                  }
                  else if(winner && p.team != winner){
                      players[id].games_lost += 1;
                  }
                  else {
                      players[id].games_tied += 1;
                      players[id].name = log.names[id];
                  }
                }
              }
              
              getSteamProfiles(steamids64).then(result => {
                for(r of result){
                  sid3 = new SteamID(r._id);
                  sid3 = sid3.getSteam3RenderedID()
                  players[sid3].avatar = r.avatar; 
                }
                res.end(JSON.stringify(players));
                client.close();
              })
            })
        }
        else if(queryObject['f'] == 5){
            matchid = queryObject['matchid']
            db.collection('logs').findOne({_id : matchid}, (cmdErr, result) => {
                res.end(JSON.stringify(result));
                client.close();
            });
        }
        else if(queryObject['f'] == 6){
            p_id = queryObject['p_id'];
            time_range = queryObject['time_range'];
            dt = new Date();
            if(time_range == 'all')dt = 1;
            else if(time_range == 'weekly')dt =  startOfWeek(dt);
            else if(time_range == 'monthly')dt =  startOfMonth(dt);
            z = new Date(dt);
            z = z.getTime()

            db.collection('logs').aggregate([
                {
                  '$match': {
                    ['players.'+p_id]: {
                      '$exists': true
                    }, 
                    'info.date': {
                      '$gt': z / 1000
                    }, 
                    'players_count': {
                      '$lt': 15
                    }
                  }
                },
                {
                    '$sort': {
                      'info.date': -1
                    }
                  },
                {
                  '$project': {
                    'map': '$info.map', 
                    'team' : '$players.'+p_id+'.team',
                    'score' : {
                        'Red' : '$teams.Red.score',
                        'Blue' : '$teams.Blue.score'
                    },
                    'result' : {
                      "$switch": {
                        "branches": [
                          { "case": 
                            { 
                              '$and': [
                                {'$eq' : ['$players.'+p_id+'.team','Red']},
                                {"$gt": [ '$teams.Red.score', '$teams.Blue.score' ]}
                              ]
                            },
                            "then": 'won' 
                          },
                          { "case": 
                            { 
                              '$and': [
                                {'$eq' : ['$players.'+p_id+'.team','Blue']},
                                {"$gt": [ '$teams.Blue.score', '$teams.Red.score' ]}
                              ]
                            },
                            "then": 'won' 
                          },
                          { "case": 
                            { 
                              '$and': [
                                {'$eq' : ['$players.'+p_id+'.team','Blue']},
                                {"$lt": [ '$teams.Blue.score', '$teams.Red.score' ]}
                              ]
                            },
                            "then": 'lost' 
                          },
                          { "case": 
                            { 
                              '$and': [
                                {'$eq' : ['$players.'+p_id+'.team','Red']},
                                {"$lt": [ '$teams.Red.score', '$teams.Blue.score' ]}
                              ]
                            },
                            "then": 'lost' 
                          }
                        ],
                        "default": 'tied'
                      }
                    },
                    'stats' : {
                        'kills' : '$players.'+p_id+'.kills',
                        'assists':'$players.'+p_id+'.assists',
                        'deaths':'$players.'+p_id+'.deaths',
                        'dmg':'$players.'+p_id+'.dmg',
                        'dapm':'$players.'+p_id+'.dapm',
                        'kpd':'$players.'+p_id+'.kpd',
                        'cpc':'$players.'+p_id+'.cpc'
                    }, 
                    'classes': {
                      '$map': {
                        'input': '$players.'+p_id+'.class_stats', 
                        'as': 'class', 
                        'in': '$$class.type'
                      }
                    }
                  }
                }
            ]).toArray((cmdErr, result) => {
                res.end(JSON.stringify(result));
                client.close();
            });
        }
        else if(queryObject['f'] == 7){
          time_range = queryObject['time_range'];
          param = queryObject['param'];
          filter_class = JSON.parse(queryObject['filter_class']);
          !filter_class ? filter_class = {'$exists' : 1} : false;
          filter_maps = JSON.parse(queryObject['filter_maps']);
          filter_maps.length == 0 ? filter_maps = {'$exists' : 1} : filter_maps = {'$in' : filter_maps};
          dt = new Date();
          if(time_range == 'all')dt = 1;
          else if(time_range == 'weekly')dt =  startOfWeek(dt);
          else if(time_range == 'monthly')dt =  startOfMonth(dt);
          z = new Date(dt);
          z = z.getTime()

          db.collection('logs').aggregate([
            {
              '$match': {
                'players_count': {
                  '$lt': 15
                }, 
                'info.date': {
                  '$gt': z / 1000
                }
              }
            }, {
              '$project': {
                'names': {
                  '$objectToArray': '$names'
                }, 
                'players_arr': {
                  '$objectToArray': '$players'
                }, 
                'map': '$info.map'
              }
            }, {
              '$unwind': {
                'path': '$players_arr'
              }
            }, {
              '$project': {
                '_id': 1, 
                'p_id': '$players_arr.k', 
                'name': {
                  '$arrayElemAt': [
                    '$names', {
                      '$indexOfArray': [
                        '$names.k', '$players_arr.k'
                      ]
                    }
                  ]
                }, 
                'class_stats': '$players_arr.v.class_stats',
                'map': '$map', 
                'value': `$players_arr.v.${param}`
              }
            }, {
              '$project': {
                '_id': 1, 
                'p_id': 1, 
                'name': '$name.v', 
                'map': 1, 
                'class_stats' : 1,
                'value': 1
              }
            },{
              '$match':{
                'map' : filter_maps,
                'class_stats.type' : filter_class
              }
            }, {
              '$sort': {
                'value': -1
              }
            }, {
              '$limit': 10
            }
          ]).toArray(async (cmdErr, result) => {
            steamids64 = [];
            for({p_id} of result){
              p_id = new SteamID(p_id).getSteamID64();
              if(!steamids64.includes(p_id))steamids64.push(p_id)
            }

            steam_profiles = await getSteamProfiles(steamids64);
            for(r of result){
              p_id = new SteamID(r.p_id).getSteamID64();
              r['m_id'] = r._id,
              r = Object.assign(r,steam_profiles.find(x => {return x._id == p_id}))
            }
            res.end(JSON.stringify(result));
            client.close();
          });
        }
        else if(queryObject['f'] == 8){
          steamids64 = JSON.parse(queryObject['steamids']);
          getSteamProfiles(steamids64).then(r => {
            res.end(JSON.stringify(r));
            client.close();
          })
        }
      })
}).listen(app_port);