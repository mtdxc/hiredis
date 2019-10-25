#include <stdlib.h>

#include <hiredis.h>
#include <async.h>
#include <adapters/glib.h>
#include <signal.h>
static GMainLoop *mainloop;
static GMainContext *context = NULL;
static redisAsyncContext *redis = NULL;
volatile char stop = 0;
void handle_signal(int signum){
  if(mainloop)
    g_main_loop_quit(mainloop);
  stop = 1;
  if(redis){
    redisAsyncDisconnect(redis);
    redis = NULL;
  }
}

static void schedule_timer(int msec, GSourceFunc func, void* userdata){
	GSource *timeout_source = g_timeout_source_new(msec);
	g_source_set_callback(timeout_source, func, userdata, NULL);
	g_source_attach(timeout_source, context);
	g_source_unref(timeout_source);
}

void redis_connect();
static gboolean timeout_reconnect(gpointer user_data){
  if(!stop)
    redis_connect();
  return G_SOURCE_REMOVE; //G_SOURCE_CONTINUE;
}
static void subscribe_cb(redisAsyncContext *ac,
           gpointer r,
           gpointer user_data);
static void
connect_cb (const redisAsyncContext *ac, int status)
{
    if (status != REDIS_OK) {
        printf("redis connect error: %s\n", ac->errstr);
        //g_main_loop_quit(mainloop);
        if(ac == redis)
          redis = NULL;
        if(!stop)
          schedule_timer(1000, timeout_reconnect, NULL);
    } else {
        printf("redis Connected...\n");
        redisAsyncCommand(ac, subscribe_cb, NULL, "subscribe talk");
        redis = ac;
    }
}

static void
disconnect_cb (const redisAsyncContext *ac G_GNUC_UNUSED,
               int status)
{
    if (status != REDIS_OK) {
        printf("redis disconnect: %s\n", ac->errstr);
        if(!stop)
          schedule_timer(1000, timeout_reconnect, NULL);
    } else {
        printf("redis Disconnected...\n");
        //g_main_loop_quit(mainloop);
    }
    // redisAsyncFree(ac);
    if(ac==redis)
      redis = NULL;
}

void redis_connect(){
  const char* server = "127.0.0.1";
  printf("connecting redis %s\n", server);
  redisAsyncContext* ac = redisAsyncConnect(server, 6379);
  if (ac->err) {
      g_printerr("%s\n", ac->errstr);
      exit(EXIT_FAILURE);
  }
  GSource *source = redis_source_new(ac);
  g_source_attach(source, context);

  redisAsyncSetConnectCallback(ac, connect_cb);
  redisAsyncSetDisconnectCallback(ac, disconnect_cb);
  redis = ac;
}

static void
subscribe_cb(redisAsyncContext *ac,
           gpointer r,
           gpointer user_data)
{
    int cmd_idx = user_data;
    redisReply *reply = r;
    if(!r){
      printf("subscribe_cb %d got null reply\n", cmd_idx);
      return ;
    }
    switch (reply->type) {
    case REDIS_REPLY_NIL:
      printf("subscribe %d reply nil\n", cmd_idx);
      break;
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
      printf("subscribe %d reply %s\n", cmd_idx, reply->str);
      break;
    case REDIS_REPLY_ERROR:
      printf("subscribe %d error %s\n", cmd_idx, reply->str);
      break;
    case REDIS_REPLY_INTEGER:
      printf( "subscribe %d reply %lld\n", cmd_idx, reply->integer);
      break;
    case REDIS_REPLY_ARRAY:
      if (reply->elements == 3) {
        if(reply->element[2]->type == REDIS_REPLY_INTEGER){
          // subscribe ok or unsubcribe
          printf("%d %s %s %lld\n", cmd_idx, reply->element[0]->str, reply->element[1]->str, reply->element[2]->integer);
        }
        else{
          // message notify
          printf("%d %s %s> %s\n", cmd_idx, reply->element[0]->str, reply->element[1]->str, reply->element[2]->str);
        }
      }
      else{
        printf( "subscribe %d reply %d array\n", cmd_idx, reply->elements);
      }
      break;
    }
}

void
command_cb(redisAsyncContext *ac,
  gpointer r,
  gpointer user_data)
{
  redisReply *reply = r;
  int cmd_idx = (int)user_data;
  if (reply) {
    switch (reply->type) {
    case REDIS_REPLY_NIL:
      printf("redis %d reply nil\n", cmd_idx);
      break;
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
      printf("redis %d reply %s\n", cmd_idx, reply->str);
      break;
    case REDIS_REPLY_ERROR:
      printf("redis %d error %s\n", cmd_idx, reply->str);
      break;
    case REDIS_REPLY_INTEGER:
      printf( "redis %d reply %lld\n", cmd_idx, reply->integer);
      break;
    case REDIS_REPLY_ARRAY:
      printf( "redis %d reply %d array\n", cmd_idx, reply->elements);
      break;
    }
  }
}

static void *loop_func(void *data) {
  g_main_loop_run(mainloop);
}

gint
main (gint argc     G_GNUC_UNUSED,
      gchar *argv[] G_GNUC_UNUSED)
{
    mainloop = g_main_loop_new(context, FALSE);
    int idx = 0;
    redis_connect();
    redisAsyncCommand(redis, command_cb, idx++, "SET key 1234");
    redisAsyncCommand(redis, command_cb, idx++, "GET key");
    signal(SIGTERM, handle_signal);

    GError *error = NULL;
	  /* Start the sessions watchdog */
	  GThread* work = g_thread_try_new("videoroom watchdog", &loop_func, NULL, &error);
    char line[256];
    while(!stop){
      if(gets(line) && redis && line[0]){
        if(strstr(line, "subscribe"))
          redisAsyncCommand(redis, subscribe_cb, idx++, line); //"subscribe %s", line);
        else
          redisAsyncCommand(redis, command_cb, idx++, line);
      }
    }
    g_thread_join(work);
    return EXIT_SUCCESS;
}
