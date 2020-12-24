
//Vue.config.devtools = true;
//uri = 'http://192.168.0.2:8081/';
uri = 'https://statypikapy.cloudno.de/';
Vue.config.devtools = false;

$('#logs_list').flowtype({
    minFont   : 13,
    maxFont   : 16
 });

$('#scoreboard').flowtype({
    minFont   : 13,
    maxFont   : 16
});
$('#player_logs_list').flowtype({
    minFont   : 13,
    maxFont   : 16
});

var log = {
    props: ['lkey'],
    template: '#log',
    data: function() {
        return {
            show_details: false,
            log_details: {
                Red : {},
                Blu : {}
            }
        }
    },
    methods: {
        toggle() {
            this.show_details = !this.show_details
            if(this.show_details){
                
                app.getLog(this.$props.lkey._id).then(r=>{
                    Red = [];
                    Blu = [];
                    for(p in r.players){
                        if(r.players[p].team == 'Red'){
                            a = [];
                            for(c of r.players[p].class_stats){
                                a.push(c.type);
                            }
                            Red.push({
                                name : r.names[p],
                                classes : a,
                                kills : r.players[p].kills,
                                assists : r.players[p].assists,
                                deaths : r.players[p].deaths,
                                dpm : r.players[p].dapm,
                            })
                        }
                        else if(r.players[p].team == 'Blue'){
                            a = [];
                            for(c of r.players[p].class_stats){
                                a.push(c.type);
                            }
                            Blu.push({
                                name : r.names[p],
                                classes : a,
                                kills : r.players[p].kills,
                                assists : r.players[p].assists,
                                deaths : r.players[p].deaths,
                                dpm : r.players[p].dapm,
                            })
                        }
                    }
                    Red.sort(function(a, b) {
                        return b.kills - a.kills;
                    });
                    Blu.sort(function(a, b) {
                        return b.kills - a.kills;
                    });
                    
                    this.log_details.Red = Red;
                    this.log_details.Blu = Blu;
                })

            }
        }
      }
};

const store = new Vuex.Store({
  state: {
    updating: false,
    logs_time_range: 'monthly', //weekly, monthly, all
  },
  mutations: {
    updateLogsTimeRange (state, value) {
        state.logs_time_range = value
    }
  }
})

var hof_card = {
    props: ['title','param'],
    template: '#hof_card',
    store,
    data: function() {
        return {
            expand_players_list : false,
            filter_class: false,
            filter_maps: [],
            test : store.state.updating,
            loading: false,
        }
    },
    methods: {
        
    },
    asyncComputed : {
        players : function (){
            if(!store.state.updating){
                this.loading = true;
                return axios.get(uri, {
                    params: {
                        f:7,
                        time_range: store.state.logs_time_range,
                        param: this.param,
                        filter_class: this.filter_class,
                        filter_maps: JSON.stringify(this.filter_maps)
                    }
                }).then(r => {
                    d = r.data;
                    if(d.length == 0)d = false;  
                    this.loading = false;
                    return d;
                })   
            }
            else return false;
        }
    }
}

var app = new Vue({
    el: '#app',
    store,
    vuetify: new Vuetify(),
    components: {
        'log': log,
        'hof-card' : hof_card 
    },
    data: {
        active_panel: 'score', //logs, score
        logs_to_show: [],
        logs_to_download : [],
        logs_to_download_total : 0,
        logs_last_sort : false,
        scores_last_sort: false,
        loading: true,
        selected_player: false,
        filter_by_map: '',
        filter_by_class: '',
        downloaded:{
            weekly : {
                scores : false,
                logs: false,
                player_logs: {}
            },
            monthly : {
                scores : false,
                logs: false,
                player_logs: {}
            },
            all : {
                scores : false,
                logs: false,
                player_logs: {}
            }
        },
        small_vw: false
    },
    computed: {
        logs_time_range: {
            get () {
              return this.$store.state.logs_time_range
            },
            set (value) {
              this.$store.commit('updateLogsTimeRange', value)
            }
        }
    },
    methods: {
        update : function (){
            fetch(uri+'?f=1').then(r => r.json()).then(r => {
                app.logs_to_download = r;
                app.logs_to_download_total = r.length
                if(r.length > 0)app.downloadLog(r[0],0)
                else {app.main();document.getElementById('update_info').innerText='All logs up to date!';console.log('All logs up to date!')};
            })
        },
        changeTimeRange : function(event){
            store.state.logs_time_range = event.target.attributes.value.value;
        },
        downloadLog : function (matchid,counter){           
            fetch(uri+'?f=2&matchid='+matchid).then(r => r.text()).then(r => {
                if(r == 'Success'){
                    
                    app.logs_to_download.shift();
                    console.log(app.logs_to_download.length)
                    document.getElementById('update_info').innerText = "Logs to download: " + app.logs_to_download.length;
                    counter++;
                    if(app.logs_to_download.length > 0){
                        if(counter < 15)app.downloadLog(app.logs_to_download[0],counter)
                        else setTimeout(function () {app.downloadLog(app.logs_to_download[0],0)},2000)
                    }
                    else {app.main()}
                }
                else console.log(r);
            })
        },
        getLog: function (matchid){
            return fetch(uri + '?f=5&matchid='+matchid).then(r => r.json())
        },
        main : function (){
            document.getElementById('update_info').style = 'display: none';
            store.state.updating = false;
            app.loading = false;
        },
        checkVW : function(){
            const vw = Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0)
            if(vw < 1080)this.small_vw = true;
            else this.small_vw = false;
        },
        selectPlayer : function (p_id){
            this.checkVW();
            app.selected_player == p_id ? app.selected_player = false : app.selected_player = p_id;
            app.filter_by_class="";
            app.filter_by_map="";
            if(document.getElementById('player_logs_head')){
                a = document.getElementById('player_logs_head').getElementsByClassName('underlined')
                if(a.length > 0)a[0].classList.remove('underlined');
            }
        },
        sortLogs : function (param) {
            a = document.getElementById('player_logs_head').getElementsByClassName('underlined')
            if(a.length > 0)a[0].classList.remove('underlined');
            event.target.classList.add('underlined');
            if(param == app.logs_last_sort){app.player_logs.reverse()}
            else ( 
                this.player_logs.sort(function(a, b){
                    return b.stats[param]-a.stats[param]
                })
            )
            app.logs_last_sort = param;
        },
        sortScores : function (param) {
            a = document.getElementById('score_head').getElementsByClassName('underlined')
            if(a.length > 0)a[0].classList.remove('underlined');
            event.target.classList.add('underlined');
            if(param == app.scores_last_sort){app.scores.reverse()}
            else (
                this.scores.sort(function(a, b){
                    return b[1][param]-a[1][param]
                })
            )
            app.scores_last_sort = param;
            if(app.selected_player)setTimeout(function () {document.querySelectorAll(`[data-sid*="${app.selected_player}"]`)[0].scrollIntoView({
                behavior: "smooth",
                block: "center"
            })},100)
            
        },
        showScoreboard : function (){
            if(this.active_panel=="score" && this.scores){
                if(this.selected_player){
                    if(!this.small_vw)return true;
                    else return false;
                }
                else return true;
            }
            else return false;
        },
        setRouteFromURL: function (){
            hash = window.location.hash.split('/')
            if(window.location.hash.length>0){
                this.logs_time_range = hash[1];
                this.active_panel = hash[2];
                this.selected_player = hash[3];
                console.log(hash[2])
            }
            else {
                window.location.hash = `/${store.state.logs_time_range}/${this.active_panel}`
            }
        },
        resizeMain : function (){
            el_main = document.getElementsByTagName('main')[0]
            panels = document.getElementsByClassName('panel')
            if(this.selected_player && this.active_panel=="score") {
                el_main.style.marginTop = "150px"
                for(el of panels){
                    el.style.height = 'calc(100vh - 150px)';
                }
            }
            else {
                el_main.style.marginTop = "100px"
                for(el of panels){
                    el.style.height = 'calc(100vh - 100px)';
                }
            }
        }
    },
    asyncComputed: {
        logs : function () {
            if(!store.state.updating){
                if(this.active_panel == 'logs'){
                    if(!this.downloaded[store.state.logs_time_range].logs){
                        this.loading = true;
                        return fetch(uri + '?f=3&time_range='+store.state.logs_time_range).then(r => r.json()).then(r => {
                            for(log of r){
                                const d = new Date(log.date * 1000);
                                options = {
                                    year: 'numeric', month: 'numeric', day: 'numeric',
                                    hour: 'numeric', minute: 'numeric',
                                    hour12: false,
                                    timeZone: 'Europe/Berlin' 
                                };
                                log.date = new Intl.DateTimeFormat('pl', options).format(d);
                            }
                            this.loading = false;
                            if(r.length == 0)r = false;  
                            this.downloaded[store.state.logs_time_range].logs = r; 
                            return r;
                        })
                    }
                    else return this.downloaded[store.state.logs_time_range].logs
                }
            }
        },
        scores : function () {
            if(!store.state.updating){
                if(this.active_panel == 'score'){
                    if(!this.downloaded[store.state.logs_time_range].scores){
                        this.loading = true;
                        return fetch(uri + '?f=4&time_range='+store.state.logs_time_range).then(r => r.json()).then(r => {
                            for(p_id in r){
                                r[p_id].score = (r[p_id].games_won*2) - (r[p_id].games_lost*2) + (r[p_id].games_tied)
                            }
                            r = sortObject(r,'score')
                            app.scores_last_sort='score';
                            this.loading = false;
                            if(r.length == 0)r = false;     
                            this.downloaded[store.state.logs_time_range].scores = r;           
                            return r;
                        })
                    }
                    else return this.downloaded[store.state.logs_time_range].scores
                }
            }
        },
        player_logs : function(){
            if(this.selected_player){
                //if(!this.downloaded[store.state.logs_time_range].player_logs[this.selected_player]){
                    this.loading = true;
                    return fetch(uri + '?f=6&p_id='+this.selected_player+'&time_range='+store.state.logs_time_range).then(r => r.json()).then(r => {
                        this.logs_last_sort = false;
                        this.loading = false;
                        if(r.length == 0)r = false;  
                        this.downloaded[store.state.logs_time_range].player_logs[this.selected_player] = r;
                        app.logs_last_sort='date' ;
                        return r;
                    })
                //}
                //else return this.downloaded[store.state.logs_time_range].player_logs[this.selected_player]
            }
        },
        filtered_player_logs: function (){
            if(this.player_logs){
                fl = app.player_logs;
                if(app.filter_by_class!=''){
                    fl = fl.filter(log => {
                        for(c of log.classes){
                            if(c == app.filter_by_class)return true;
                        }
                    })
                }
                if(app.filter_by_map!=''){
                    fl = fl.filter((log) => {
                        if(log.map.includes(app.filter_by_map))return true
                    })
                }
                return fl;
            }
        }
    },
    filters: {
        sliceFirstAndLast: function (value) {
          if (!value) return '';
          else return value.slice(1,-1);
        }
    },
    watch: {
        logs_time_range : function(str) {
                hash = window.location.hash.split('/');hash[0] = '#';hash[1] = str;window.location.hash = hash.join('/');
        },
        active_panel : function(str) {
                
        },
        selected_player : function(str) {
                hash = window.location.hash.split('/');hash[0] = '#';
                if(str)hash[3] = str;
                else hash[3] = '';
                window.location.hash = hash.join('/');
                this.resizeMain();
        },
        active_panel : {handler : function (str){
            hash = window.location.hash.split('/');hash[0] = '#';hash[2] = str;
            if(str =='logs')hash[3] = '';
            else if(str =='score' && this.selected_player)hash[3] = this.selected_player;
            window.location.hash = hash.join('/');
            this.resizeMain()
        },immediate: false}
    },
    created: function (){
        window.onhashchange = this.setRouteFromURL;
        this.resizeMain()
        this.setRouteFromURL()
        this.checkVW();
        this.update(); 
    }
})

function sortObject(obj,param){
    var newObject = {};
        var sortable=[];
        for(var key in obj){
            if(obj.hasOwnProperty(key))sortable.push([key, obj[key]]);
            sortable.sort(function(a, b){
            return b[1][param]-a[1][param]})
        }
        return sortable; //returns array not object!
}
