#include "redis.h"
#include "bio.h"

#define LEVELDB_KEY_FLAG_DATABASE_ID 0
#define LEVELDB_KEY_FLAG_TYPE 1 
#define LEVELDB_KEY_FLAG_SET_KEY_LEN 2
#define LEVELDB_KEY_FLAG_SET_KEY 3

void initleveldb(struct leveldb* ldb, char *path) {
  ldb->options = leveldb_options_create();
  leveldb_options_set_create_if_missing(ldb->options, 1);
  leveldb_options_set_compression(ldb->options, 1);
  leveldb_options_set_write_buffer_size(ldb->options, 64 * 1024 * 1024);
  leveldb_options_set_max_open_files(ldb->options, 500);

  char *err = NULL;
  ldb->db = leveldb_open(ldb->options, path, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "open leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    exit(1);
  }

  ldb->roptions = leveldb_readoptions_create();
  leveldb_readoptions_set_fill_cache(ldb->roptions, 0);

  ldb->woptions = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(ldb->woptions, 0);
}

int addFreezedKey(int dbid, sds key, char keytype) {
    dictEntry *entry = dictAddRaw(server.db[dbid].freezed,key);

    if (!entry) return REDIS_ERR;
    dictSetSignedIntegerVal(entry, keytype);
    return REDIS_OK;
}

int loadFreezedKey(struct leveldb* ldb) {
    int success = REDIS_OK;
    int dbid = 0;
    unsigned long len = 0;
    char *data = NULL;
    char *value = NULL;
    size_t dataLen = 0;
    size_t valueLen = 0;
    sds strkey;
    int retval;
    leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
    char tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN];

    tmp[LEVELDB_KEY_FLAG_TYPE] = 'f';
    for(dbid = 0; dbid < server.dbnum; dbid++) {
        tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
        
        for(leveldb_iter_seek(iterator, tmp, LEVELDB_KEY_FLAG_SET_KEY_LEN); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
            data = (char*) leveldb_iter_key(iterator, &dataLen);
            if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid || data[LEVELDB_KEY_FLAG_TYPE] != 'f') break;

            value = (char*) leveldb_iter_value(iterator, &valueLen);
            if(valueLen != 1) continue;
            
            len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
            strkey = sdsnewlen(data+LEVELDB_KEY_FLAG_SET_KEY,len);
            retval = addFreezedKey(dbid, strkey, value[0]);
            redisAssertWithInfo(NULL,NULL,retval == REDIS_OK);
        }
    }
    
    char *err = NULL;
    leveldb_iter_get_error(iterator, &err);
    if(err != NULL) {
        redisLog(REDIS_WARNING, "load freezedkey iterator err: %s", err);
        leveldb_free(err);
        success = REDIS_ERR;
        err = NULL;
    }

    leveldb_iter_destroy(iterator);
    return success;
}

int isKeyFreezed(int dbid, robj *key) {
    dictEntry *de = dictFind(server.db[dbid].freezed,key->ptr);
		return de ? 1 : 0;
}

sds createleveldbFreezedKeyHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'f';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  return key;
}

int freezeKey(redisDb *db, struct leveldb *ldb, robj *key, char keytype) {
    sds strkey;
    sds leveldbkey;
    int retval;
    char *err = NULL;
    
    if (!dbDelete(db, key)) {
	    return REDIS_ERR;
    }

    leveldbkey = createleveldbFreezedKeyHead(db->id, key->ptr);
    leveldb_put(ldb->db, ldb->woptions, leveldbkey, sdslen(leveldbkey), &keytype, 1, &err);
    if (err != NULL) {
        redisLog(REDIS_WARNING, "freezekey err: %s", err);
        leveldb_free(err);
        sdsfree(leveldbkey);
        err = NULL;
        return REDIS_ERR;
    }
    server.leveldb_op_num++;
    sdsfree(leveldbkey);
    
    strkey = sdsdup(key->ptr);
    retval = addFreezedKey(db->id, strkey, keytype);
    redisAssertWithInfo(NULL,key,retval == REDIS_OK);
    if(retval != REDIS_OK)
    {
        sdsfree(strkey);
        return REDIS_ERR;
    }
    
    return REDIS_OK;
}

int callCommandForleveldb(struct redisClient *fakeClient, char *data, size_t dataLen, leveldb_iterator_t *iterator) {
    char *value = NULL;
    size_t valueLen = 0;
    int argc;
    unsigned long len;
    robj **argv;
    struct redisCommand *cmd;
    int tmptype;

    tmptype = data[LEVELDB_KEY_FLAG_TYPE];
    if(tmptype == 'h'){
        argc = 4;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("hset",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
        value = (char*) leveldb_iter_value(iterator, &valueLen);
        argv[3] = createStringObject(value, valueLen);
    }else if(tmptype == 's'){
        argc = 3;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("sadd",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
    }else if(tmptype == 'z'){
        argc = 4;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("zadd",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[3] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
        value = (char*) leveldb_iter_value(iterator, &valueLen);
        argv[2] = createStringObject(value, valueLen);
    }else{
        redisLog(REDIS_WARNING,"callCommandForLeveldb no found type: %d %d", fakeClient->db->id, tmptype);
        freeFakeClientArgv(fakeClient);
        return REDIS_ERR;
    }

    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
        redisLog(REDIS_WARNING,"Unknown command '%s' from leveldb", (char*)argv[0]->ptr);
        exit(1);
    }
    cmd->proc(fakeClient);

    /* The fake client should not have a reply */
    redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    freeFakeClientArgv(fakeClient);
    
    return REDIS_OK;
}

int meltKey(int dbid, struct leveldb *ldb, robj *key, char keytype) {
    int success = 1;
    char *data = NULL;
    size_t dataLen = 0;
    size_t keylen = sdslen(key->ptr);
    sds leveldbkey;
    sds sdskey;
    char *err = NULL;
    struct redisClient *fakeClient = ldb->fakeClient;

    if(selectDb(fakeClient,dbid) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "meltKey select db error: %d", dbid);
        return REDIS_ERR;
    }
    
    leveldbkey = createleveldbFreezedKeyHead(dbid, key->ptr);
    leveldb_delete(ldb->db, ldb->woptions, leveldbkey, sdslen(leveldbkey), &err);
    if (err != NULL) {
        redisLog(REDIS_WARNING, "meltKey leveldb err: %s", err);
        leveldb_free(err); 
				sdsfree(leveldbkey);
        err = NULL;
        return REDIS_ERR;
    }
    sdsfree(leveldbkey);
    server.leveldb_op_num++;
    
    dictDelete(server.db[dbid].freezed,key->ptr);

    char tmp[LEVELDB_KEY_FLAG_SET_KEY];
    tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
    tmp[LEVELDB_KEY_FLAG_TYPE] = keytype;
    tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = keylen;
    sdskey = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);
    sdskey = sdscatsds(sdskey, key->ptr);
    
    leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
    for(leveldb_iter_seek(iterator, sdskey, sdslen(sdskey)); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
        data = (char*) leveldb_iter_key(iterator, &dataLen);
        if(data[LEVELDB_KEY_FLAG_SET_KEY_LEN] != (char)keylen) break;
        if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
        if(memcmp(key->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, keylen) != 0) break;
        
        if (callCommandForleveldb(fakeClient, data, dataLen, iterator) == REDIS_OK) {
            server.dirty++;
        } else {
            success = 0;
            break;
        }
    }
    
    sdsfree(sdskey);
    
    leveldb_iter_get_error(iterator, &err);
    if(err != NULL) {
        redisLog(REDIS_WARNING, "meltKey iterator err: %s", err);
        leveldb_free(err);
        err = NULL;
    }

    leveldb_iter_destroy(iterator);
    if(success == 1) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

int loadleveldb(char *path) {
  redisLog(REDIS_NOTICE, "load leveldb path: %s", path);

  struct redisClient *fakeClient = createFakeClient();
  int old_leveldb_state = server.leveldb_state;
  long loops = 0;

  server.leveldb_state = REDIS_LEVELDB_OFF;
  initleveldb(&server.ldb, path);
  server.ldb.fakeClient = fakeClient;
  
  if(loadFreezedKey(&server.ldb) == REDIS_ERR) {
    server.leveldb_state = old_leveldb_state;
    return REDIS_ERR;
  }
  
  startLoading(NULL);

  int success = 1;
  int dbid = 0;
  char *data = NULL;
  size_t dataLen = 0;
  int tmpdbid;
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    unsigned long len;
    robj *tmpkey;

    if (!(loops++ % 1000000)) {
      processEventsWhileBlocked();
      redisLog(REDIS_NOTICE, "load leveldb: %lu", loops);
    }
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    tmpdbid = data[LEVELDB_KEY_FLAG_DATABASE_ID];
    if(tmpdbid != dbid) {
      if(selectDb(fakeClient,tmpdbid) == REDIS_OK ){
        dbid = tmpdbid;
      }else{
        redisLog(REDIS_WARNING, "load leveldb select db error: %d", tmpdbid);
        success = 0;
        break;
      }
    }
    
    len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    tmpkey = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
    if(isKeyFreezed(dbid, tmpkey) == 1) {
      decrRefCount(tmpkey);
      continue;
    }
    decrRefCount(tmpkey);
    
    if (callCommandForleveldb(fakeClient, data, dataLen, iterator) == REDIS_ERR) {
      success = 0;
      break;
    }
  }
  redisLog(REDIS_NOTICE, "load leveldb sum: %lu", loops);

  stopLoading();
  server.leveldb_state = old_leveldb_state;

  char *err = NULL;
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "load leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }

  leveldb_iter_destroy(iterator);
  if(success == 1) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

void closeleveldb(struct leveldb *ldb) {
  leveldb_writeoptions_destroy(ldb->woptions);
  leveldb_readoptions_destroy(ldb->roptions);
  leveldb_options_destroy(ldb->options);
  leveldb_close(ldb->db);
  freeFakeClient(ldb->fakeClient);
}

sds createleveldbHashHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'h';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbHset(int dbid, struct leveldb *ldb, robj** argv) {
  leveldbHsetDirect(dbid, ldb, argv[1], argv[2], argv[3]);
}

void leveldbHsetDirect(int dbid, struct leveldb *ldb, robj *argv1, robj *argv2, robj *argv3) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  robj *r3 = getDecodedObject(argv3);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatsds(key, r2->ptr);
  leveldb_put(ldb->db, ldb->woptions, key, sdslen(key), r3->ptr, sdslen(r3->ptr), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hset direct leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
  decrRefCount(r3);
}

void leveldbHmset(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i += 2) {
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j+1]->ptr, sdslen(rs[j+1]->ptr));
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hmset leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbHdel(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "hdel leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbHclear(int dbid, struct leveldb *ldb, robj *argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbHashHead(dbid, r1->ptr);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "hclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "hclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

sds createleveldbSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 's';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbSadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), NULL, 0);
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "sadd leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbSrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++ ) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "srem leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbSclear(int dbid, struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "sclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "sclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

sds createleveldbSortedSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'z';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

void leveldbZadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i += 2) {
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    key = sdscatsds(key, rs[j+1]->ptr);
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j]->ptr, sdslen(rs[j]->ptr));
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zadd leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbZaddDirect(int dbid, struct leveldb *ldb, robj* argv1, robj* argv2, double score) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  key = sdscatsds(key, r2->ptr);
  size_t klen = sdslen(key);
  char *err = NULL;
  char buf[128];
  int len = snprintf(buf,sizeof(buf),"%.17g",score);

  leveldb_put(ldb->db, ldb->woptions, key, klen, buf, len, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zadd direct leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
}

void leveldbZrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv[1]);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  for (i = 2; i < argc; i++ ) {
    rs[j] = getDecodedObject(argv[i]);
    key = sdscatsds(key, rs[j]->ptr);
    leveldb_writebatch_delete(wb, key, sdslen(key));
    sdsrange(key, 0, klen - 1);
    j++;
  }
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_writebatch_destroy(wb);
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  zfree(rs);
}

void leveldbZremByLongLong(int dbid, struct leveldb *ldb, robj *arg, long long vlong) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;
  char buf[64];
  int len;

  len = ll2string(buf,64,vlong);
  key = sdscatlen(key, buf, len);
  //redisLog(REDIS_NOTICE, "leveldbZremByLongLong %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by long long leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
}

void leveldbZremByCBuffer(int dbid, struct leveldb *ldb, robj *arg, unsigned char *vstr, unsigned int vlen) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatlen(key, vstr, vlen);
  //redisLog(REDIS_NOTICE, "leveldbZremByCBuffer %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by c buffer leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
}

void leveldbZremByObject(int dbid, struct leveldb *ldb, robj *arg, robj *field) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(arg);
  robj *r2 = getDecodedObject(field);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  key = sdscatsds(key,  r2->ptr);
  //redisLog(REDIS_NOTICE, "leveldbZremByObject %s", key);
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "zrem by object leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  decrRefCount(r2);
}

void leveldbZclear(int dbid, struct leveldb *ldb, robj* argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  robj *r1 = getDecodedObject(argv);
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "zclear leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  sdsfree(key);
  decrRefCount(r1);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "zclear leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void leveldbFlushdb(int dbid, struct leveldb* ldb) {
  char tmp[1];
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  for(leveldb_iter_seek(iterator, tmp, 1); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "flushdb leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "flushdb leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void leveldbFlushall(struct leveldb* ldb) {
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    if (err != NULL) {
      redisLog(REDIS_WARNING, "flushall leveldb err: %s", err);
      leveldb_free(err); 
      err = NULL;
    }
  }
  server.leveldb_op_num++;

  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "flushall leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
  }
  leveldb_iter_destroy(iterator);
}

void backupleveldb(void *arg) {
  char *path = (char *)arg;
  time_t backup_start = time(NULL);
  leveldb_options_t *options = leveldb_options_create();
  int success = 0;

  leveldb_options_set_create_if_missing(options, 1);
  leveldb_options_set_error_if_exists(options, 1);
  leveldb_options_set_compression(options, 1);
  leveldb_options_set_write_buffer_size(options, 64 * 1024 * 1024);

  char *err = NULL;
  leveldb_t *db = leveldb_open(options, path, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "open leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    goto cleanup;
  }

  char *data = NULL;
  size_t dataLen = 0;
  char *value = NULL;
  size_t valueLen = 0;
  int i = 0;
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  leveldb_writeoptions_t *woptions = leveldb_writeoptions_create();
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  leveldb_writeoptions_set_sync(woptions, 0);
  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    value = (char*) leveldb_iter_value(iterator, &valueLen);
    leveldb_writebatch_put(wb, data, dataLen, value, valueLen);
    i++;
    if(i == 1000) {
      i = 0;
      leveldb_write(db, woptions, wb, &err);
      if (err != NULL) {
        redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
        leveldb_free(err); 
        err = NULL;
        goto closehandler;
      }
      leveldb_writebatch_destroy(wb);
      wb = leveldb_writebatch_create();
    }
  }
  leveldb_write(db, woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
    leveldb_free(err); 
    err = NULL;
    goto closehandler;
  }
  leveldb_writebatch_destroy(wb);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "backup leveldb iterator err: %s", err);
    leveldb_free(err);
    err = NULL;
    goto closehandler;
  }
  success = 1;
closehandler:
  leveldb_iter_destroy(iterator);
  leveldb_writeoptions_destroy(woptions);
  leveldb_close(db);
cleanup:
  leveldb_options_destroy(options);
  if( success == 1) {
    time_t backup_end = time(NULL);
    char info[1024];
    char tmpfile[512];
    char backupfile[512];
    snprintf(tmpfile,512,"%s/temp.log", path);
    snprintf(backupfile,512,"%s/BACKUP.log", path);
    FILE *fp = fopen(tmpfile,"w");

    if (!fp) {
      redisLog(REDIS_WARNING, "Failed opening .log for saving: %s",
          strerror(errno));
    }else{
      int infolen = snprintf(info, sizeof(info), "BACKUP\n\tSTART:\t%jd\n\tEND:\t\t%jd\n\tCOST:\t\t%jd\nSUCCESS", backup_start, backup_end, backup_end-backup_start);

      fwrite(info, infolen, 1, fp);
      fclose(fp);
      if (rename(tmpfile,backupfile) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp backup file on the final destination: %s", strerror(errno));
      }
      unlink(tmpfile);
      redisLog(REDIS_NOTICE, "backup leveldb path: %s", path);
    }
  }
  zfree(path);
}

void backupCommand(redisClient *c) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    addReplyError(c,"leveldb off");
    return;
  }

  size_t len = sdslen(c->argv[1]->ptr);
  char *path = zmalloc(len + 1);
  memcpy(path, c->argv[1]->ptr, len);
  path[len] = '\0';
  bioCreateBackgroundJob(REDIS_BIO_LEVELDB_BACKUP,(void*)path,NULL,NULL);
  addReplyStatus(c,"backup leveldb started");
}

void freezeCommand(redisClient *c) {
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    int deleted = 0;
    int j;
    robj *o;
    
    for (j = 1; j < c->argc; j++) {
        o = lookupKeyRead(c->db,c->argv[j]);
        if (o != NULL) {
            if(o->type == REDIS_SET) {
                if(freezeKey(c->db, &server.ldb, c->argv[j], 's') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze set key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else if(o->type == REDIS_ZSET) {
                if(freezeKey(c->db, &server.ldb, c->argv[j], 'z') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze zset key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else if(o->type == REDIS_HASH) {
                if(freezeKey(c->db, &server.ldb, c->argv[j], 'h') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze hash key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else {
                redisLog(REDIS_WARNING, "freezeCommand keytype not set or zset or hash");
                continue;
            }
            signalModifiedKey(c->db,c->argv[j]);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[j],c->db->id);
            server.dirty++;
            deleted++;
        }
    }
    
    addReplyLongLong(c,deleted);
}

char getFreezedKeyType(int dbid, robj *key) {
    dictEntry *de = dictFind(server.db[dbid].freezed,key->ptr);

    if (de) {
        return (char)(dictGetSignedIntegerVal(de));
    }
    return 0;
}

void meltCommand(redisClient *c) {
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    int success = 0;
    int j;
    
    for (j = 1; j < c->argc; j++) {
        if(isKeyFreezed(c->db->id, c->argv[j]) == 1) {
            if(meltKey(c->db->id, &server.ldb, c->argv[j], getFreezedKeyType(c->db->id, c->argv[j])) == REDIS_OK) {
                success++;
            } else {
                redisLog(REDIS_WARNING, "meltCommand melt key:%s failed", (char*)c->argv[j]->ptr);
            }
        }
    }
    
    addReplyLongLong(c,success);
}

void freezedCommand(redisClient *c) {
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(c->db->freezed);
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        char keytype = (char)(dictGetSignedIntegerVal(de));;
        robj *keyobj;
        robj *keyval;

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            keyval = createStringObject(&keytype,1);
            addReplyBulk(c,keyobj);
            addReplyBulk(c,keyval);
            numkeys++;
            numkeys++;
            decrRefCount(keyobj);
            decrRefCount(keyval);
        }
    }
    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c,replylen,numkeys);
}
