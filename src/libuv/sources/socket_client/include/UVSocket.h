
/***************************************

* @file uvsocket.h

* @brief 基于libuv封装的tcp服务器与客户端,使用log4z作日志工具

* @details

* @author ppshuai

* @date 2016-12-26

* 修正服务器与客户端的错误.现服务器支持多客户端连接

修改客户端测试代码，支持并发多客户端测试

****************************************/

#ifndef __UVSOCKET_H
#define __UVSOCKET_H

#include <stdlib.h>
#include <iostream>
#include <string>
#include <stdstring.h>
#include <list>
#include <map>
#include <log4z.h>
#include <uv.h>

#define BUFFERSIZE (1024*1024)

namespace uvsocket
{
    typedef void (*newconnect)(int clientid);
    typedef void (*server_recvcb)(int cliendid, const char* buf, int bufsize);
    typedef void (*client_recvcb)(const char* buf, int bufsize, void* userdata);

    class CTCPServer;
    class CSockData
    {
    public:

        CSockData(int clientid):client_id(clientid),recvcb_(0/*nullptr*/) {

            client_handle = (uv_tcp_t*)malloc(sizeof(*client_handle));
            client_handle->data = this;
            readbuffer = uv_buf_init((char*)malloc(BUFFERSIZE), BUFFERSIZE);
            writebuffer = uv_buf_init((char*)malloc(BUFFERSIZE), BUFFERSIZE);
        }

        virtual ~CSockData() {
            free(readbuffer.base);
            readbuffer.base = 0/*nullptr*/;
            readbuffer.len = 0;
            free(writebuffer.base);
            writebuffer.base = 0/*nullptr*/;
            writebuffer.len = 0;
            free(client_handle);
            client_handle = 0/*nullptr*/;
        }

        int client_id;//客户端id,惟一
        uv_tcp_t* client_handle;//客户端句柄
        CTCPServer* tcp_server;//服务器句柄(保存是因为某些回调函数需要到)
        uv_buf_t readbuffer;//接受数据的buf
        uv_buf_t writebuffer;//写数据的buf
        uv_write_t write_req;
        server_recvcb recvcb_;//接收数据回调给用户的函数
    };
    typedef std::map<int,CSockData*>    INTSOCKDATAMAP;
    typedef INTSOCKDATAMAP::iterator    INTSOCKDATAMAPIT;
    typedef INTSOCKDATAMAP::value_type  INTSOCKDATAMAPPAIR;
    class CTCPServer
    {
    public:

        CTCPServer(uv_loop_t* loop = uv_default_loop());
        virtual ~CTCPServer();
        static void StartLog(const char* logpath = 0);//启动日志，必须启动才会生成日志

    public:
        //基本函数
        bool Start(const char *ip, int port);//启动服务器,地址为IP4
        bool Start6(const char *ip, int port);//启动服务器，地址为IP6
        void close();
        bool setNoDelay(bool enable);
        bool setKeepAlive(int enable, unsigned int delay);
        const char* GetLastErrMsg() const {
            return errmsg_.c_str();
        };

        virtual int send(int clientid, const char* data, std::size_t len);
        virtual void setnewconnectcb(newconnect cb);
        virtual void setrecvcb(int clientid,server_recvcb cb);//设置接收回调函数，每个客户端各有一个

    protected:
        int GetAvailaClientID()const;//获取可用的client id
        bool DeleteClient(int clientid);//删除链表中的客户端
        //静态回调函数
        static void AfterServerRecv(uv_stream_t *client, ssize_t nread, const uv_buf_t* buf);
        static void AfterSend(uv_write_t *req, int status);
        static void onAllocBuffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
        static void AfterServerClose(uv_handle_t *handle);
        static void AfterClientClose(uv_handle_t *handle);
        static void acceptConnection(uv_stream_t *server, int status);

    private:
        bool init();
        bool run(int status = UV_RUN_DEFAULT);
        bool bind(const char* ip, int port);
        bool bind6(const char* ip, int port);
        bool listen(int backlog = 1024);
        uv_tcp_t server_;//服务器链接
        INTSOCKDATAMAP clients_list_;//子客户端链接
        uv_mutex_t mutex_handle_;//保护clients_list_
        uv_loop_t *loop_;
        std::string errmsg_;
        newconnect newconcb_;
        bool isinit_;//是否已初始化，用于close函数中判断
    };

    class CTCPClient
    {
    //直接调用connect/connect6会进行连接
    public:
        CTCPClient(uv_loop_t* loop = uv_default_loop());
        virtual ~CTCPClient();
        static void StartLog(const char* logpath = 0/*nullptr*/);//启动日志，必须启动才会生成日志
    public:
        //基本函数
        virtual bool connect(const char* ip, int port);//启动connect线程，循环等待直到connect完成
        virtual bool connect6(const char* ip, int port);//启动connect线程，循环等待直到connect完成
        virtual int send(const char* data, std::size_t len);
        virtual void setrecvcb(client_recvcb cb, void* userdata);////设置接收回调函数，只有一个
        void close();
        //是否启用Nagle算法
        bool setNoDelay(bool enable);
        bool setKeepAlive(int enable, unsigned int delay);
        const char* GetLastErrMsg() const {
            return errmsg_.c_str();
        };

    protected:
        //静态回调函数
        static void AfterConnect(uv_connect_t* handle, int status);
        static void AfterClientRecv(uv_stream_t *client, ssize_t nread, const uv_buf_t* buf);
        static void AfterSend(uv_write_t *req, int status);
        static void onAllocBuffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
        static void AfterClose(uv_handle_t *handle);
        static void ConnectThread(void* arg);//真正的connect线程
        static void ConnectThread6(void* arg);//真正的connect线程
        bool init();
        bool run(int status = UV_RUN_DEFAULT);
    private:
        enum {
            CONNECT_TIMEOUT,
            CONNECT_FINISH,
            CONNECT_ERROR,
            CONNECT_DIS,
        };
        uv_tcp_t client_;//客户端连接
        uv_loop_t *loop_;
        uv_write_t write_req_;//写时请求
        uv_connect_t connect_req_;//连接时请求
        uv_thread_t connect_threadhanlde_;//线程句柄
        std::string errmsg_;//错误信息
        uv_buf_t readbuffer_;//接受数据的buf
        uv_buf_t writebuffer_;//写数据的buf
        uv_mutex_t write_mutex_handle_;//保护write,保存前一write完成才进行下一write
        int connectstatus_;//连接状态
        client_recvcb recvcb_;//回调函数
        void* userdata_;//回调函数的用户数据
        std::string connectip_;//连接的服务器IP
        int connectport_;//连接的服务器端口号
        bool isinit_;//是否已初始化，用于close函数中判断
    };
}

#endif // __UVSOCKET_H
