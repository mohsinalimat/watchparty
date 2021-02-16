"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Room = void 0;
const config_1 = __importDefault(require("./config"));
const crypto_1 = __importDefault(require("crypto"));
const zlib_1 = __importDefault(require("zlib"));
const util_1 = __importDefault(require("util"));
const axios_1 = __importDefault(require("axios"));
const ioredis_1 = __importDefault(require("ioredis"));
const pg_1 = require("pg");
const firebase_1 = require("./utils/firebase");
const redis_1 = require("./utils/redis");
const stripe_1 = require("./utils/stripe");
const time_1 = require("./utils/time");
const utils_1 = require("./vm/utils");
const postgres_1 = require("./utils/postgres");
const gzip = util_1.default.promisify(zlib_1.default.gzip);
let redis = undefined;
if (config_1.default.REDIS_URL) {
    redis = new ioredis_1.default(config_1.default.REDIS_URL);
}
let postgres = undefined;
if (config_1.default.DATABASE_URL) {
    postgres = new pg_1.Client({
        connectionString: config_1.default.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    });
    postgres.connect();
}
class Room {
    constructor(io, roomId, roomData) {
        // Serialized state
        this.video = '';
        this.videoTS = 0;
        this.subtitle = '';
        this.paused = false;
        this.chat = [];
        this.nameMap = {};
        this.pictureMap = {};
        this.vBrowser = undefined;
        this.creationTime = new Date();
        this.lock = undefined; // uid of the user who locked the room
        this.roster = [];
        this.tsMap = {};
        this.clientIdMap = {};
        this.uidMap = {};
        this.roomRedis = undefined;
        this.tsInterval = undefined;
        this.isChatDisabled = undefined;
        this.serialize = () => {
            // Get the set of IDs with messages in chat
            // Only serialize roster and picture ID for those people, to save space
            const chatIDs = new Set(this.chat.map((msg) => msg.id));
            const abbrNameMap = {};
            Object.keys(this.nameMap).forEach((id) => {
                if (chatIDs.has(id)) {
                    abbrNameMap[id] = this.nameMap[id];
                }
            });
            const abbrPictureMap = {};
            Object.keys(this.pictureMap).forEach((id) => {
                if (chatIDs.has(id)) {
                    abbrPictureMap[id] = this.pictureMap[id];
                }
            });
            return JSON.stringify({
                video: this.video,
                videoTS: this.videoTS,
                subtitle: this.subtitle,
                paused: this.paused,
                chat: this.chat,
                nameMap: abbrNameMap,
                pictureMap: abbrPictureMap,
                vBrowser: this.vBrowser,
                creationTime: this.creationTime,
                lock: this.lock,
            });
        };
        this.deserialize = (roomData) => {
            const roomObj = JSON.parse(roomData);
            this.video = roomObj.video;
            this.videoTS = roomObj.videoTS;
            if (roomObj.subtitle) {
                this.subtitle = roomObj.subtitle;
            }
            if (roomObj.paused) {
                this.paused = roomObj.paused;
            }
            if (roomObj.chat) {
                this.chat = roomObj.chat;
            }
            if (roomObj.nameMap) {
                this.nameMap = roomObj.nameMap;
            }
            if (roomObj.pictureMap) {
                this.pictureMap = roomObj.pictureMap;
            }
            if (roomObj.vBrowser) {
                this.vBrowser = roomObj.vBrowser;
            }
            if (roomObj.creationTime) {
                this.creationTime = new Date(roomObj.creationTime);
            }
            if (roomObj.lock) {
                this.lock = roomObj.lock;
            }
        };
        this.saveToRedis = (permanent) => __awaiter(this, void 0, void 0, function* () {
            if (config_1.default.ENABLE_POSTGRES_SAVING && postgres) {
                try {
                    const roomString = this.serialize();
                    yield (postgres === null || postgres === void 0 ? void 0 : postgres.query(`UPDATE room SET "lastUpdateTime" = $1, data = $2 WHERE "roomId" = $3`, [new Date(), roomString, this.roomId]));
                }
                catch (e) {
                    console.warn(e);
                }
            }
            if (!redis) {
                return;
            }
            try {
                const roomString = this.serialize();
                const key = this.roomId;
                if (permanent === null) {
                    yield (redis === null || redis === void 0 ? void 0 : redis.set(key, roomString, 'KEEPTTL'));
                }
                else if (permanent === true) {
                    yield (redis === null || redis === void 0 ? void 0 : redis.set(key, roomString));
                    yield (redis === null || redis === void 0 ? void 0 : redis.persist(key));
                }
                else if (permanent === false) {
                    yield (redis === null || redis === void 0 ? void 0 : redis.setex(key, 24 * 60 * 60, roomString));
                }
            }
            catch (e) {
                console.warn(e);
            }
        });
        this.destroy = () => {
            var _a;
            if (this.tsInterval) {
                clearInterval(this.tsInterval);
            }
            if (this.roomRedis) {
                (_a = this.roomRedis) === null || _a === void 0 ? void 0 : _a.disconnect();
                this.roomRedis = undefined;
            }
        };
        this.getHostState = () => {
            // Reverse lookup the clientid to the socket id
            const match = this.roster.find((user) => { var _a; return this.clientIdMap[user.id] === ((_a = this.vBrowser) === null || _a === void 0 ? void 0 : _a.controllerClient); });
            return {
                video: this.video,
                videoTS: this.videoTS,
                subtitle: this.subtitle,
                paused: this.paused,
                isVBrowserLarge: Boolean(this.vBrowser && this.vBrowser.large),
                controller: match === null || match === void 0 ? void 0 : match.id,
            };
        };
        this.stopVBrowserInternal = () => __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c, _d, _e, _f, _g, _h;
            (_a = this.roomRedis) === null || _a === void 0 ? void 0 : _a.disconnect();
            this.roomRedis = undefined;
            const assignTime = this.vBrowser && this.vBrowser.assignTime;
            const id = (_b = this.vBrowser) === null || _b === void 0 ? void 0 : _b.id;
            const provider = (_d = (_c = this.vBrowser) === null || _c === void 0 ? void 0 : _c.provider) !== null && _d !== void 0 ? _d : config_1.default.VM_MANAGER_ID;
            const isLarge = (_f = (_e = this.vBrowser) === null || _e === void 0 ? void 0 : _e.large) !== null && _f !== void 0 ? _f : false;
            const region = (_h = (_g = this.vBrowser) === null || _g === void 0 ? void 0 : _g.region) !== null && _h !== void 0 ? _h : '';
            this.vBrowser = undefined;
            this.cmdHost(undefined, '');
            // Force a save to record the vbrowser change
            this.saveToRedis(null);
            if (redis && assignTime) {
                yield redis.lpush('vBrowserSessionMS', Number(new Date()) - assignTime);
                yield redis.ltrim('vBrowserSessionMS', 0, 49);
            }
            if (id) {
                try {
                    const vmManager = utils_1.getVMManager(provider, isLarge, region);
                    yield (vmManager === null || vmManager === void 0 ? void 0 : vmManager.resetVM(id));
                }
                catch (e) {
                    console.warn(e);
                }
            }
        });
        this.cmdHost = (socket, data) => {
            this.video = data;
            this.videoTS = 0;
            this.paused = false;
            this.subtitle = '';
            this.tsMap = {};
            this.io.of(this.roomId).emit('REC:tsMap', this.tsMap);
            this.io.of(this.roomId).emit('REC:host', this.getHostState());
            if (socket && data) {
                const chatMsg = { id: socket.id, cmd: 'host', msg: data };
                this.addChatMessage(socket, chatMsg);
            }
        };
        this.addChatMessage = (socket, chatMsg) => {
            if (this.isChatDisabled && !chatMsg.cmd) {
                return;
            }
            const chatWithTime = Object.assign(Object.assign({}, chatMsg), { timestamp: new Date().toISOString(), videoTS: socket ? this.tsMap[socket.id] : undefined });
            this.chat.push(chatWithTime);
            this.chat = this.chat.splice(-100);
            this.io.of(this.roomId).emit('REC:chat', chatWithTime);
        };
        this.validateLock = (socketId) => {
            if (!this.lock) {
                return true;
            }
            const result = this.uidMap[socketId] === this.lock;
            if (!result) {
                console.log('[VALIDATELOCK] failed', socketId);
            }
            return result;
        };
        this.validateOwner = (uid) => __awaiter(this, void 0, void 0, function* () {
            var _j;
            const result = yield (postgres === null || postgres === void 0 ? void 0 : postgres.query('SELECT owner FROM room where "roomId" = $1', [this.roomId]));
            const owner = (_j = result === null || result === void 0 ? void 0 : result.rows[0]) === null || _j === void 0 ? void 0 : _j.owner;
            return !owner || uid === owner;
        });
        this.changeUserName = (socket, data) => {
            if (!data) {
                return;
            }
            if (data && data.length > 50) {
                return;
            }
            this.nameMap[socket.id] = data;
            this.io.of(this.roomId).emit('REC:nameMap', this.nameMap);
        };
        this.changeUserPicture = (socket, data) => {
            if (data && data.length > 10000) {
                return;
            }
            this.pictureMap[socket.id] = data;
            this.io.of(this.roomId).emit('REC:pictureMap', this.pictureMap);
        };
        this.changeUserID = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            if (!data) {
                return;
            }
            const decoded = yield firebase_1.validateUserToken(data.uid, data.token);
            if (!decoded) {
                return;
            }
            this.uidMap[socket.id] = decoded.uid;
        });
        this.startHosting = (socket, data) => {
            if (data && data.length > 20000) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            const sharer = this.roster.find((user) => user.isScreenShare);
            if (sharer || this.vBrowser) {
                // Can't update the video while someone is screensharing/filesharing or vbrowser is running
                return;
            }
            redis_1.redisCount('urlStarts');
            this.cmdHost(socket, data);
        };
        this.playVideo = (socket) => {
            var _a;
            if (!this.validateLock(socket.id)) {
                return;
            }
            socket.broadcast.emit('REC:play', this.video);
            const chatMsg = {
                id: socket.id,
                cmd: 'play',
                msg: (_a = this.tsMap[socket.id]) === null || _a === void 0 ? void 0 : _a.toString(),
            };
            this.paused = false;
            this.addChatMessage(socket, chatMsg);
        };
        this.pauseVideo = (socket) => {
            var _a;
            if (!this.validateLock(socket.id)) {
                return;
            }
            socket.broadcast.emit('REC:pause');
            const chatMsg = {
                id: socket.id,
                cmd: 'pause',
                msg: (_a = this.tsMap[socket.id]) === null || _a === void 0 ? void 0 : _a.toString(),
            };
            this.paused = true;
            this.addChatMessage(socket, chatMsg);
        };
        this.seekVideo = (socket, data) => {
            if (String(data).length > 100) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            this.videoTS = data;
            socket.broadcast.emit('REC:seek', data);
            const chatMsg = { id: socket.id, cmd: 'seek', msg: data === null || data === void 0 ? void 0 : data.toString() };
            this.addChatMessage(socket, chatMsg);
        };
        this.setTimestamp = (socket, data) => {
            if (String(data).length > 100) {
                return;
            }
            if (data > this.videoTS) {
                this.videoTS = data;
            }
            this.tsMap[socket.id] = data;
        };
        this.sendChatMessage = (socket, data) => {
            if (data && data.length > 10000) {
                return;
            }
            if (config_1.default.NODE_ENV === 'development' && data === '/clear') {
                this.chat.length = 0;
                this.io.of(this.roomId).emit('chatinit', this.chat);
                return;
            }
            redis_1.redisCount('chatMessages');
            const chatMsg = { id: socket.id, msg: data };
            this.addChatMessage(socket, chatMsg);
        };
        this.joinVideo = (socket) => {
            const match = this.roster.find((user) => user.id === socket.id);
            if (match) {
                match.isVideoChat = true;
                redis_1.redisCount('videoChatStarts');
            }
            this.io.of(this.roomId).emit('roster', this.roster);
        };
        this.leaveVideo = (socket) => {
            const match = this.roster.find((user) => user.id === socket.id);
            if (match) {
                match.isVideoChat = false;
            }
            this.io.of(this.roomId).emit('roster', this.roster);
        };
        this.joinScreenSharing = (socket, data) => {
            if (!this.validateLock(socket.id)) {
                return;
            }
            const sharer = this.roster.find((user) => user.isScreenShare);
            if (sharer) {
                // Someone's already sharing
                return;
            }
            if (data && data.file) {
                this.cmdHost(socket, 'fileshare://' + socket.id);
                redis_1.redisCount('fileShareStarts');
            }
            else {
                this.cmdHost(socket, 'screenshare://' + socket.id);
                redis_1.redisCount('screenShareStarts');
            }
            const match = this.roster.find((user) => user.id === socket.id);
            if (match) {
                match.isScreenShare = true;
            }
            this.io.of(this.roomId).emit('roster', this.roster);
        };
        this.leaveScreenSharing = (socket) => {
            const sharer = this.roster.find((user) => user.isScreenShare);
            if (!sharer || (sharer === null || sharer === void 0 ? void 0 : sharer.id) !== socket.id) {
                return;
            }
            sharer.isScreenShare = false;
            this.cmdHost(socket, '');
            this.io.of(this.roomId).emit('roster', this.roster);
        };
        this.startVBrowser = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            var _k, _l, _m, _o, _p, _q, _r, _s;
            if (this.vBrowser || this.roomRedis) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                socket.emit('errorMessage', 'Room is locked.');
                return;
            }
            const decoded = yield firebase_1.validateUserToken(data.uid, data.token);
            if (!decoded) {
                socket.emit('errorMessage', 'Invalid user token.');
                return;
            }
            if (!data) {
                socket.emit('errorMessage', 'Invalid input.');
                return;
            }
            const clientId = this.clientIdMap[socket.id];
            const uid = this.uidMap[socket.id];
            // Log the vbrowser creation by uid and clientid
            if (redis) {
                const expireTime = time_1.getStartOfDay() / 1000 + 86400;
                if (clientId) {
                    const clientCount = yield redis.zincrby('vBrowserClientIDs', 1, clientId);
                    redis.expireat('vBrowserClientIDs', expireTime);
                    const clientMinutes = yield redis.zincrby('vBrowserClientIDMinutes', 1, clientId);
                    redis.expireat('vBrowserClientIDMinutes', expireTime);
                    console.log(clientId, clientCount, clientMinutes);
                }
                if (uid) {
                    const uidCount = yield redis.zincrby('vBrowserUIDs', 1, uid);
                    redis.expireat('vBrowserUIDs', expireTime);
                    const uidMinutes = yield redis.zincrby('vBrowserUIDMinutes', 1, uid);
                    redis.expireat('vBrowserUIDMinutes', expireTime);
                    console.log(uid, uidCount, uidMinutes);
                }
                // TODO limit users based on these counts
            }
            let isLarge = false;
            let region = '';
            if (config_1.default.STRIPE_SECRET_KEY && data && data.uid && data.token) {
                const decoded = yield firebase_1.validateUserToken(data.uid, data.token);
                // Check if user is subscriber, if so allow isLarge
                if (decoded === null || decoded === void 0 ? void 0 : decoded.email) {
                    const customer = yield stripe_1.getCustomerByEmail(decoded.email);
                    if (((_m = (_l = (_k = customer === null || customer === void 0 ? void 0 : customer.subscriptions) === null || _k === void 0 ? void 0 : _k.data) === null || _l === void 0 ? void 0 : _l[0]) === null || _m === void 0 ? void 0 : _m.status) === 'active') {
                        console.log('found active sub for ', customer === null || customer === void 0 ? void 0 : customer.email);
                        isLarge = ((_o = data.options) === null || _o === void 0 ? void 0 : _o.size) === 'large';
                        region = (_p = data.options) === null || _p === void 0 ? void 0 : _p.region;
                    }
                }
            }
            if (config_1.default.RECAPTCHA_SECRET_KEY) {
                try {
                    // Validate the request isn't spam/automated
                    const validation = yield axios_1.default({
                        url: `https://www.google.com/recaptcha/api/siteverify?secret=${config_1.default.RECAPTCHA_SECRET_KEY}&response=${data.rcToken}`,
                        method: 'POST',
                    });
                    // console.log(validation?.data);
                    const isLowScore = ((_q = validation === null || validation === void 0 ? void 0 : validation.data) === null || _q === void 0 ? void 0 : _q.score) < 0.2;
                    const failed = ((_r = validation === null || validation === void 0 ? void 0 : validation.data) === null || _r === void 0 ? void 0 : _r.success) === false;
                    if (failed || isLowScore) {
                        if (isLowScore) {
                            redis_1.redisCount('recaptchaRejectsLowScore');
                        }
                        else {
                            redis_1.redisCount('recaptchaRejectsOther');
                        }
                        socket.emit('errorMessage', 'Invalid ReCAPTCHA.');
                        return;
                    }
                }
                catch (e) {
                    // if Recaptcha is down or other network issues, allow continuing
                    console.warn(e);
                }
            }
            redis_1.redisCount('vBrowserStarts');
            this.cmdHost(socket, 'vbrowser://');
            const vmManager = utils_1.getVMManager(config_1.default.VM_MANAGER_ID, isLarge, region);
            if (!vmManager) {
                socket.emit('errorMessage', 'Server is not configured properly for VBrowsers.');
                return;
            }
            this.roomRedis = new ioredis_1.default(config_1.default.REDIS_URL);
            const seconds = utils_1.getSessionLimitSeconds(isLarge);
            const assignment = yield utils_1.assignVM(this.roomRedis, vmManager, seconds);
            if (!this.roomRedis) {
                // Maybe the user cancelled the request before assignment finished
                return;
            }
            (_s = this.roomRedis) === null || _s === void 0 ? void 0 : _s.disconnect();
            this.roomRedis = undefined;
            if (!assignment) {
                this.cmdHost(socket, '');
                this.vBrowser = undefined;
                socket.emit('errorMessage', 'Failed to assign VBrowser. Please try again later.');
                redis_1.redisCount('vBrowserFails');
                return;
            }
            this.vBrowser = assignment;
            this.vBrowser.controllerClient = clientId;
            this.vBrowser.creatorUID = uid;
            this.vBrowser.creatorClientID = clientId;
            this.cmdHost(undefined, 'vbrowser://' + this.vBrowser.pass + '@' + this.vBrowser.host);
        });
        this.stopVBrowser = (socket) => __awaiter(this, void 0, void 0, function* () {
            if (!this.vBrowser && !this.roomRedis && this.video !== 'vbrowser://') {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            yield this.stopVBrowserInternal();
            redis_1.redisCount('vBrowserTerminateManual');
        });
        this.changeController = (socket, data) => {
            if (data && data.length > 100) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            if (this.vBrowser) {
                this.vBrowser.controllerClient = this.clientIdMap[data];
                this.io.of(this.roomId).emit('REC:changeController', data);
            }
        };
        this.addSubtitles = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            if (data && data.length > 1000000) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            if (!redis) {
                return;
            }
            // calculate hash, gzip and save to redis
            const hash = crypto_1.default
                .createHash('sha256')
                .update(data, 'utf8')
                .digest()
                .toString('hex');
            const gzipData = (yield gzip(data));
            // console.log(data.length, gzipData.length);
            yield redis.setex('subtitle:' + hash, 3 * 60 * 60, gzipData);
            this.subtitle = hash;
            this.io.of(this.roomId).emit('REC:subtitle', this.subtitle);
            redis_1.redisCount('subUploads');
        });
        this.lockRoom = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            if (!data) {
                return;
            }
            const decoded = yield firebase_1.validateUserToken(data.uid, data.token);
            if (!decoded) {
                return;
            }
            if (!this.validateLock(socket.id)) {
                return;
            }
            this.lock = data.locked ? decoded.uid : '';
            this.io.of(this.roomId).emit('REC:lock', this.lock);
            const chatMsg = {
                id: socket.id,
                cmd: data.locked ? 'lock' : 'unlock',
                msg: '',
            };
            this.addChatMessage(socket, chatMsg);
        });
        this.setRoomOwner = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            var _t, _u, _v, _w;
            if (!postgres) {
                socket.emit('errorMessage', 'Database is not available');
                return;
            }
            const decoded = yield firebase_1.validateUserToken(data === null || data === void 0 ? void 0 : data.uid, data === null || data === void 0 ? void 0 : data.token);
            if (!decoded) {
                socket.emit('errorMessage', 'Failed to authenticate user');
                return;
            }
            const owner = decoded.uid;
            const isOwner = yield this.validateOwner(decoded.uid);
            if (!isOwner) {
                socket.emit('errorMessage', 'Not current room owner');
                return;
            }
            const customer = yield stripe_1.getCustomerByEmail(decoded.email);
            const isSubscriber = Boolean(((_v = (_u = (_t = customer === null || customer === void 0 ? void 0 : customer.subscriptions) === null || _t === void 0 ? void 0 : _t.data) === null || _u === void 0 ? void 0 : _u[0]) === null || _v === void 0 ? void 0 : _v.status) === 'active');
            if (data.undo) {
                if (config_1.default.ENABLE_POSTGRES_SAVING) {
                    yield postgres_1.updateObject(postgres, 'room', {
                        password: null,
                        owner: null,
                        vanity: null,
                        isChatDisabled: null,
                        isSubRoom: null,
                    }, { roomId: this.roomId });
                }
                else {
                    yield postgres.query('DELETE from room where "roomId" = $1', [
                        this.roomId,
                    ]);
                }
                socket.emit('REC:getRoomState', {});
                this.saveToRedis(false);
            }
            else {
                // validate room count
                const roomCount = (yield postgres.query('SELECT count(1) from room where owner = $1 AND "roomId" != $2', [owner, this.roomId])).rows[0].count;
                const limit = isSubscriber ? 10 : 1;
                // console.log(roomCount, limit, isSubscriber);
                if (roomCount >= limit) {
                    socket.emit('errorMessage', `You've exceeded the permanent room limit. Subscribe for additional permanent rooms.`);
                    return;
                }
                // Only keep the rows for which we have a postgres column
                const roomObj = {
                    roomId: this.roomId,
                    creationTime: this.creationTime,
                    owner: owner,
                    isSubRoom: isSubscriber,
                };
                let result = null;
                result = yield postgres_1.upsertObject(postgres, 'room', roomObj, {
                    roomId: this.roomId,
                });
                const row = (_w = result === null || result === void 0 ? void 0 : result.rows) === null || _w === void 0 ? void 0 : _w[0];
                // console.log(result, row);
                socket.emit('REC:getRoomState', {
                    password: row === null || row === void 0 ? void 0 : row.password,
                    vanity: row === null || row === void 0 ? void 0 : row.vanity,
                    owner: row === null || row === void 0 ? void 0 : row.owner,
                });
                this.saveToRedis(true);
            }
        });
        this.getRoomState = (socket) => __awaiter(this, void 0, void 0, function* () {
            if (!postgres) {
                return;
            }
            const result = yield postgres.query(`SELECT password, vanity, owner, "isChatDisabled" FROM room where "roomId" = $1`, [this.roomId]);
            const first = result.rows[0];
            if (this.isChatDisabled === undefined) {
                this.isChatDisabled = Boolean(first === null || first === void 0 ? void 0 : first.isChatDisabled);
            }
            // TODO only send the password if this is current owner
            socket.emit('REC:getRoomState', {
                password: first === null || first === void 0 ? void 0 : first.password,
                vanity: first === null || first === void 0 ? void 0 : first.vanity,
                owner: first === null || first === void 0 ? void 0 : first.owner,
                isChatDisabled: first === null || first === void 0 ? void 0 : first.isChatDisabled,
            });
        });
        this.setRoomState = (socket, data) => __awaiter(this, void 0, void 0, function* () {
            var _x, _y, _z;
            if (!postgres) {
                socket.emit('errorMessage', 'Database is not available');
                return;
            }
            const decoded = yield firebase_1.validateUserToken(data === null || data === void 0 ? void 0 : data.uid, data === null || data === void 0 ? void 0 : data.token);
            if (!decoded) {
                socket.emit('errorMessage', 'Failed to authenticate user');
                return;
            }
            const isOwner = yield this.validateOwner(decoded.uid);
            if (!isOwner) {
                socket.emit('errorMessage', 'Not current room owner');
                return;
            }
            const customer = yield stripe_1.getCustomerByEmail(decoded.email);
            const isSubscriber = Boolean(((_z = (_y = (_x = customer === null || customer === void 0 ? void 0 : customer.subscriptions) === null || _x === void 0 ? void 0 : _x.data) === null || _y === void 0 ? void 0 : _y[0]) === null || _z === void 0 ? void 0 : _z.status) === 'active');
            const { password, vanity, isChatDisabled } = data;
            if (password) {
                if (password.length > 100) {
                    socket.emit('errorMessage', 'Password too long');
                    return;
                }
            }
            if (vanity) {
                if (vanity.length > 100) {
                    socket.emit('errorMessage', 'Custom URL too long');
                    return;
                }
            }
            // console.log(owner, vanity, password);
            const roomObj = {
                roomId: this.roomId,
                password: password,
                isChatDisabled: isChatDisabled,
            };
            if (isSubscriber) {
                // user must be sub to set vanity
                roomObj.vanity = vanity;
            }
            try {
                const query = `UPDATE room
        SET ${Object.keys(roomObj).map((k, i) => `"${k}" = $${i + 1}`)}
        WHERE "roomId" = $${Object.keys(roomObj).length + 1}
        AND owner = $${Object.keys(roomObj).length + 2}
        RETURNING *`;
                const result = yield postgres.query(query, [
                    ...Object.values(roomObj),
                    this.roomId,
                    decoded.uid,
                ]);
                const row = result.rows[0];
                this.isChatDisabled = Boolean(row === null || row === void 0 ? void 0 : row.isChatDisabled);
                // TODO only send password if current owner
                this.io.of(this.roomId).emit('REC:getRoomState', {
                    password: row === null || row === void 0 ? void 0 : row.password,
                    vanity: row === null || row === void 0 ? void 0 : row.vanity,
                    owner: row === null || row === void 0 ? void 0 : row.owner,
                    isChatDisabled: row === null || row === void 0 ? void 0 : row.isChatDisabled,
                });
                socket.emit('successMessage', 'Saved admin settings');
            }
            catch (e) {
                console.warn(e);
            }
        });
        this.sendSignal = (socket, data) => {
            if (!data) {
                return;
            }
            this.io
                .of(this.roomId)
                .to(data.to)
                .emit('signal', { from: socket.id, msg: data.msg });
        };
        this.signalSS = (socket, data) => {
            if (!data) {
                return;
            }
            this.io.of(this.roomId).to(data.to).emit('signalSS', {
                from: socket.id,
                sharer: data.sharer,
                msg: data.msg,
            });
        };
        this.disconnectUser = (socket) => {
            let index = this.roster.findIndex((user) => user.id === socket.id);
            const removed = this.roster.splice(index, 1)[0];
            this.io.of(this.roomId).emit('roster', this.roster);
            if (removed.isScreenShare) {
                // Reset the room state since we lost the screen sharer
                this.cmdHost(socket, '');
            }
            delete this.tsMap[socket.id];
            // delete nameMap[socket.id];
        };
        this.roomId = roomId;
        this.io = io;
        if (roomData) {
            this.deserialize(roomData);
        }
        this.tsInterval = setInterval(() => {
            // console.log(roomId, this.video, this.roster, this.tsMap, this.nameMap);
            if (this.video) {
                io.of(roomId).emit('REC:tsMap', this.tsMap);
            }
        }, 1000);
        io.of(roomId).use((socket, next) => __awaiter(this, void 0, void 0, function* () {
            var _0, _1, _2;
            // Validate the connector has the room password
            const password = (_0 = socket.handshake.query) === null || _0 === void 0 ? void 0 : _0.password;
            // console.log(this.roomId, this.password, password);
            if (postgres) {
                const result = yield postgres.query(`SELECT password, "isSubRoom" FROM room where "roomId" = $1`, [this.roomId]);
                const roomPassword = (_1 = result.rows[0]) === null || _1 === void 0 ? void 0 : _1.password;
                if (roomPassword && password !== roomPassword) {
                    next(new Error('not authorized'));
                    return;
                }
                const isSubRoom = (_2 = result.rows[0]) === null || _2 === void 0 ? void 0 : _2.isSubRoom;
                const roomCapacity = isSubRoom
                    ? config_1.default.ROOM_CAPACITY_SUB
                    : config_1.default.ROOM_CAPACITY;
                if (roomCapacity && this.roster.length >= roomCapacity) {
                    next(new Error('room full'));
                    return;
                }
            }
            next();
        }));
        io.of(roomId).on('connection', (socket) => {
            var _a;
            const clientId = (_a = socket.handshake.query) === null || _a === void 0 ? void 0 : _a.clientId;
            this.roster.push({ id: socket.id });
            this.clientIdMap[socket.id] = clientId;
            redis_1.redisCount('connectStarts');
            redis_1.redisCountDistinct('connectStartsDistinct', clientId);
            socket.emit('REC:host', this.getHostState());
            socket.emit('REC:nameMap', this.nameMap);
            socket.emit('REC:pictureMap', this.pictureMap);
            socket.emit('REC:tsMap', this.tsMap);
            socket.emit('REC:lock', this.lock);
            socket.emit('chatinit', this.chat);
            this.getRoomState(socket);
            io.of(roomId).emit('roster', this.roster);
            socket.on('CMD:name', (data) => this.changeUserName(socket, data));
            socket.on('CMD:picture', (data) => this.changeUserPicture(socket, data));
            socket.on('CMD:uid', (data) => this.changeUserID(socket, data));
            socket.on('CMD:host', (data) => this.startHosting(socket, data));
            socket.on('CMD:play', () => this.playVideo(socket));
            socket.on('CMD:pause', () => this.pauseVideo(socket));
            socket.on('CMD:seek', (data) => this.seekVideo(socket, data));
            socket.on('CMD:ts', (data) => this.setTimestamp(socket, data));
            socket.on('CMD:chat', (data) => this.sendChatMessage(socket, data));
            socket.on('CMD:joinVideo', () => this.joinVideo(socket));
            socket.on('CMD:leaveVideo', () => this.leaveVideo(socket));
            socket.on('CMD:joinScreenShare', (data) => this.joinScreenSharing(socket, data));
            socket.on('CMD:leaveScreenShare', () => this.leaveScreenSharing(socket));
            socket.on('CMD:startVBrowser', (data) => this.startVBrowser(socket, data));
            socket.on('CMD:stopVBrowser', () => this.stopVBrowser(socket));
            socket.on('CMD:changeController', (data) => this.changeController(socket, data));
            socket.on('CMD:subtitle', (data) => this.addSubtitles(socket, data));
            socket.on('CMD:lock', (data) => this.lockRoom(socket, data));
            socket.on('CMD:askHost', () => {
                socket.emit('REC:host', this.getHostState());
            });
            socket.on('CMD:getRoomState', () => this.getRoomState(socket));
            socket.on('CMD:setRoomState', (data) => this.setRoomState(socket, data));
            socket.on('CMD:setRoomOwner', (data) => this.setRoomOwner(socket, data));
            socket.on('signal', (data) => this.sendSignal(socket, data));
            socket.on('signalSS', (data) => this.signalSS(socket, data));
            socket.on('disconnect', () => this.disconnectUser(socket));
        });
    }
}
exports.Room = Room;
