"use strict";
module.exports = {
    apps: [
        {
            name: 'server',
            script: './buildServer/server.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
            env: {
                PORT: 80,
            },
        },
        {
            name: 'shard1',
            script: './buildServer/server.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
            env: {
                SHARD: 1,
                PORT: 3001,
            },
        },
        {
            name: 'shard2',
            script: './buildServer/server.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
            env: {
                SHARD: 2,
                PORT: 3002,
            },
        },
        {
            name: 'vmWorker',
            script: './buildServer/vmWorker.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
        },
        {
            name: 'syncSubs',
            script: './buildServer/syncSubs.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
        },
        {
            name: 'timeSeries',
            script: './buildServer/timeSeries.js',
            log_date_format: 'YYYY-MM-DD HH:mm Z',
        },
    ],
};