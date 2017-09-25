#include <UVSocket.h>

std::string GetUVError(int retcode)
{
    std::string err;
    err = uv_err_name(retcode);
    err +=":";
    err += uv_strerror(retcode);
    return /*std::move*/(err);
}

namespace uvsocket
{
    /*****************************************TCP Server*************************************************************/

    CTCPServer::CTCPServer(uv_loop_t* loop) : newconcb_(0/*nullptr*/), isinit_(false)
    {
        loop_ = loop;
    }

    CTCPServer::~CTCPServer()
    {
        close();
        LOGI("tcp server exit.");
    }

    //初始化与关闭--服务器与客户端一致
    bool CTCPServer::init()
    {
        if (isinit_) {
            return true;
        }

        if (!loop_) {
            errmsg_ = "loop is null on tcp init.";
            LOGE(errmsg_);
            return false;
        }

        int iret = uv_mutex_init(&mutex_handle_);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        iret = uv_tcp_init(loop_,&server_);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        isinit_ = true;
        server_.data = this;
        //iret = uv_tcp_keepalive(&server_, 1, 60);//调用此函数后后续函数会调用出错

        //if (iret) {

        //    errmsg_ = GetUVError(iret);

        //    return false;

        //}
        return true;
    }

    void CTCPServer::close()
    {
        for (INTSOCKDATAMAPIT it = clients_list_.begin(); it!=clients_list_.end(); ++it) {
            CSockData * data = it->second;
            uv_close((uv_handle_t*)data->client_handle,AfterClientClose);
        }

        clients_list_.clear();

        LOGI("close server");

        if (isinit_) {
            uv_close((uv_handle_t*)&server_, AfterServerClose);
            LOGI("close server");
			isinit_ = false;
			uv_mutex_destroy(&mutex_handle_);
        }        
    }

    bool CTCPServer::run(int status)
    {
        LOGI("server runing.");
        int iret = uv_run(loop_,(uv_run_mode)status);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }
    //属性设置--服务器与客户端一致
    bool CTCPServer::setNoDelay(bool enable)
    {
        int iret = uv_tcp_nodelay(&server_, enable ? 1 : 0);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    bool CTCPServer::setKeepAlive(int enable, unsigned int delay)
    {
        int iret = uv_tcp_keepalive(&server_, enable , delay);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    //作为server时的函数
    bool CTCPServer::bind(const char* ip, int port)
    {
        struct sockaddr_in bind_addr;
        int iret = uv_ip4_addr(ip, port, &bind_addr);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        iret = uv_tcp_bind(&server_, (const struct sockaddr*)&bind_addr,0);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        LOGI("server bind ip="<<ip<<", port="<<port);
        return true;
    }

    bool CTCPServer::bind6(const char* ip, int port)
    {
        struct sockaddr_in6 bind_addr;
        int iret = uv_ip6_addr(ip, port, &bind_addr);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        iret = uv_tcp_bind(&server_, (const struct sockaddr*)&bind_addr,0);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        LOGI("server bind ip="<<ip<<", port="<<port);
        return true;
    }

    bool CTCPServer::listen(int backlog)
    {
        int iret = uv_listen((uv_stream_t*) &server_, backlog, acceptConnection);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        LOGI("server listen");
        return true;
    }


    bool CTCPServer::Start( const char *ip, int port )
    {
        close();
        if (!init()) {
            return false;
        }

        if (!bind(ip,port)) {
            return false;
        }

        if (!listen(SOMAXCONN)) {
            return false;
        }

        if (!run()) {
            return false;
        }
        LOGI("start listen "<<ip<<": "<<port);
        return true;
    }

    bool CTCPServer::Start6( const char *ip, int port )
    {
        close();
        if (!init()) {
            return false;
        }

        if (!bind6(ip,port)) {
            return false;
        }

        if (!listen(SOMAXCONN)) {
            return false;
        }

        if (!run()) {
            return false;
        }
        return true;
    }

    //服务器发送函数
    int CTCPServer::send(int clientid, const char* data, std::size_t len)
    {
        INTSOCKDATAMAPIT itfind = clients_list_.find(clientid);
        if (itfind == clients_list_.end()) {
            errmsg_ = "can't find cliendid ";
            errmsg_ += std::to_string((long long)clientid);
            LOGE(errmsg_);
            return -1;
        }

        //自己控制data的生命周期直到write结束
        if (itfind->second->writebuffer.len < len) {
            itfind->second->writebuffer.base = (char*)realloc(itfind->second->writebuffer.base,len);
            itfind->second->writebuffer.len = len;
        }

        memcpy(itfind->second->writebuffer.base,data,len);
        uv_buf_t buf = uv_buf_init((char*)itfind->second->writebuffer.base,len);
        int iret = uv_write(&itfind->second->write_req, (uv_stream_t*)itfind->second->client_handle, &buf, 1, AfterSend);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        return true;
    }

    //服务器-新客户端函数
    void CTCPServer::acceptConnection(uv_stream_t *server, int status)
    {
        if (!server->data) {
            return;
        }

        CTCPServer *tcpsock = (CTCPServer *)server->data;
        int clientid = tcpsock->GetAvailaClientID();
        CSockData* cdata = new CSockData(clientid);//uv_close回调函数中释放
        cdata->tcp_server = tcpsock;//保存服务器的信息
        int iret = uv_tcp_init(tcpsock->loop_, cdata->client_handle);//析构函数释放
        if (iret) {
            delete cdata;
            tcpsock->errmsg_ = GetUVError(iret);
            LOGE(tcpsock->errmsg_);
            return;
        }

        iret = uv_accept((uv_stream_t*)&tcpsock->server_, (uv_stream_t*) cdata->client_handle);
        if ( iret) {
            tcpsock->errmsg_ = GetUVError(iret);
            uv_close((uv_handle_t*) cdata->client_handle, NULL);
            delete cdata;
            LOGE(tcpsock->errmsg_);
            return;
        }

        tcpsock->clients_list_.insert(std::make_pair(clientid,cdata));//加入到链接队列
        if (tcpsock->newconcb_) {
            tcpsock->newconcb_(clientid);
        }

        LOGI("new client("<<cdata->client_handle<<") id="<< clientid);
        iret = uv_read_start((uv_stream_t*)cdata->client_handle, onAllocBuffer, AfterServerRecv);//服务器开始接收客户端的数据
        return;
    }

    //服务器-接收数据回调函数
    void CTCPServer::setrecvcb(int clientid, server_recvcb cb )
    {
        INTSOCKDATAMAPIT itfind = clients_list_.find(clientid);
        if (itfind != clients_list_.end()) {
            itfind->second->recvcb_ = cb;
        }
    }

    //服务器-新链接回调函数
    void CTCPServer::setnewconnectcb(newconnect cb )
    {
        newconcb_ = cb;
    }

    //服务器分析空间函数
    void CTCPServer::onAllocBuffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
    {
        if (!handle->data) {
            return;
        }

        CSockData *client = (CSockData*)handle->data;
        *buf = client->readbuffer;
    }

    void CTCPServer::AfterServerRecv(uv_stream_t *handle, ssize_t nread, const uv_buf_t* buf)
    {
        if (!handle->data) {
            return;
        }

        CSockData *client = (CSockData*)handle->data;//服务器的recv带的是clientdata
        if (nread < 0) {/* Error or EOF */
            CTCPServer *server = (CTCPServer *)client->tcp_server;
            if (nread == UV_EOF) {
                fprintf(stdout,"客户端(%d)连接断开，关闭此客户端\n",client->client_id);
                LOGW("客户端("<<client->client_id<<")主动断开");
            } else if (nread == UV_ECONNRESET) {
                fprintf(stdout,"客户端(%d)异常断开\n",client->client_id);
                LOGW("客户端("<<client->client_id<<")异常断开");
            } else {
                fprintf(stdout,"%s\n",GetUVError(nread).c_str());
                LOGW("客户端("<<client->client_id<<")异常断开："<<GetUVError(nread));
            }

            server->DeleteClient(client->client_id);//连接断开，关闭客户端
            return;
        } else if (0 == nread) {/* Everything OK, but nothing read. */

        } else if (client->recvcb_) {
            client->recvcb_(client->client_id,buf->base,nread);
        }
    }

    //服务器与客户端一致
    void CTCPServer::AfterSend(uv_write_t *req, int status)
    {
        if (status < 0) {
            LOGE("发送数据有误:"<<GetUVError(status));
            fprintf(stderr, "Write error %s\n", GetUVError(status).c_str());
        }
    }

    void CTCPServer::AfterServerClose(uv_handle_t *handle)
    {
        //服务器,不需要做什么
    }

    void CTCPServer::AfterClientClose(uv_handle_t *handle)
    {
        CSockData *cdata = (CSockData*)handle->data;
        LOGI("client "<<cdata->client_id<<" had closed.");
        delete cdata;
    }

    int CTCPServer::GetAvailaClientID() const
    {
        static int s_id = 0;
        return ++s_id;
    }

    bool CTCPServer::DeleteClient( int clientid )
    {
        uv_mutex_lock(&mutex_handle_);
        INTSOCKDATAMAPIT itfind = clients_list_.find(clientid);
        if (itfind == clients_list_.end()) {
            errmsg_ = "can't find client ";
            errmsg_ += std::to_string((long long)clientid);
            LOGE(errmsg_);
            uv_mutex_unlock(&mutex_handle_);
            return false;
        }

        if (uv_is_active((uv_handle_t*)itfind->second->client_handle)) {
            uv_read_stop((uv_stream_t*)itfind->second->client_handle);
        }

        uv_close((uv_handle_t*)itfind->second->client_handle,AfterClientClose);

        clients_list_.erase(itfind);
        LOGI("删除客户端"<<clientid);
        uv_mutex_unlock(&mutex_handle_);
        return true;
    }

    void CTCPServer::StartLog( const char* logpath /*= nullptr*/ )
    {
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerMonthdir(LOG4Z_MAIN_LOGGER_ID, true);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerDisplay(LOG4Z_MAIN_LOGGER_ID,false);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerLevel(LOG4Z_MAIN_LOGGER_ID,LOG_LEVEL_DEBUG);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerLimitsize(LOG4Z_MAIN_LOGGER_ID,100);
        if (logpath) {
            zsummer::log4z::ILog4zManager::getInstance()->setLoggerPath(LOG4Z_MAIN_LOGGER_ID,logpath);
        }
        zsummer::log4z::ILog4zManager::getInstance()->start();
    }


    /*****************************************TCP Client*************************************************************/
    CTCPClient::CTCPClient(uv_loop_t* loop)
        :recvcb_(0),userdata_(0)
        ,connectstatus_(CONNECT_DIS)
        , isinit_(false)
    {
        readbuffer_ = uv_buf_init((char*) malloc(BUFFERSIZE), BUFFERSIZE);
        writebuffer_ = uv_buf_init((char*) malloc(BUFFERSIZE), BUFFERSIZE);
        loop_ = loop;
        connect_req_.data = this;
        write_req_.data = this;
    }

    CTCPClient::~CTCPClient()
    {
        free(readbuffer_.base);
        readbuffer_.base = 0/*nullptr*/;
        readbuffer_.len = 0;
        free(writebuffer_.base);
        writebuffer_.base = 0/*nullptr*/;
        writebuffer_.len = 0;
        close();
        LOGI("客户端("<<this<<")退出");
    }

    //初始化与关闭--服务器与客户端一致
    bool CTCPClient::init()
    {
        if (isinit_) {
            return true;
        }

        if (!loop_) {
            errmsg_ = "loop is null on tcp init.";
            LOGE(errmsg_);
            return false;
        }

        int iret = uv_tcp_init(loop_,&client_);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        iret = uv_mutex_init(&write_mutex_handle_);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        isinit_ = true;
        fprintf(stdout,"客户端(%p) init type = %d\n",&client_,client_.type);
        client_.data = this;
        //iret = uv_tcp_keepalive(&client_, 1, 60);//
        //if (iret) {
        // errmsg_ = GetUVError(iret);
        // return false;
        //}
        LOGI("客户端("<<this<<")Init");
        return true;
    }

    void CTCPClient::close()
    {
        if (!isinit_) {
            return;
        }

        uv_mutex_destroy(&write_mutex_handle_);
        uv_close((uv_handle_t*) &client_, AfterClose);
        LOGI("客户端("<<this<<")close");
        isinit_ = false;
    }

    bool CTCPClient::run(int status)
    {
        LOGI("客户端("<<this<<")run");
        int iret = uv_run(loop_,(uv_run_mode)status);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    //属性设置--服务器与客户端一致
    bool CTCPClient::setNoDelay(bool enable)
    {
        //http://blog.csdn.net/u011133100/article/details/21485983
        int iret = uv_tcp_nodelay(&client_, enable ? 1 : 0);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    bool CTCPClient::setKeepAlive(int enable, unsigned int delay)
    {
        int iret = uv_tcp_keepalive(&client_, enable , delay);
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    //作为client的connect函数
    bool CTCPClient::connect(const char* ip, int port)
    {
        close();
        init();
        connectip_ = ip;
        connectport_ = port;
        LOGI("客户端("<<this<<")start connect to server("<<ip<<":"<<port<<")");
        int iret = uv_thread_create(&connect_threadhanlde_, ConnectThread, this);//触发AfterConnect才算真正连接成功，所以用线程
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }

        while ( connectstatus_ == CONNECT_DIS) {
#if defined (WIN32) || defined(_WIN32)
            Sleep(100);
#else
            usleep((100) * 1000)
#endif
        }
        return connectstatus_ == CONNECT_FINISH;
    }

    bool CTCPClient::connect6(const char* ip, int port)
    {
        close();
        init();
        connectip_ = ip;
        connectport_ = port;
        LOGI("客户端("<<this<<")start connect to server("<<ip<<":"<<port<<")");
        int iret = uv_thread_create(&connect_threadhanlde_, ConnectThread6, this);//触发AfterConnect才算真正连接成功，所以用线程
        if (iret) {
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        while ( connectstatus_ == CONNECT_DIS) {
            //fprintf(stdout,"client(%p) wait, connect status %d\n",this,connectstatus_);
#if defined (WIN32) || defined(_WIN32)
            Sleep(100);
#else
            usleep((100) * 1000)
#endif
        }
        return connectstatus_ == CONNECT_FINISH;
    }

    void CTCPClient::ConnectThread( void* arg )
    {
        CTCPClient *pclient = (CTCPClient*)arg;
        fprintf(stdout,"client(%p) ConnectThread start\n",pclient);
        struct sockaddr_in bind_addr;
        int iret = uv_ip4_addr(pclient->connectip_.c_str(), pclient->connectport_, &bind_addr);
        if (iret) {
            pclient->errmsg_ = GetUVError(iret);
            LOGE(pclient->errmsg_);
            return;
        }
        iret = uv_tcp_connect(&pclient->connect_req_, &pclient->client_, (const sockaddr*)&bind_addr, AfterConnect);
        if (iret) {
            pclient->errmsg_ = GetUVError(iret);
            LOGE(pclient->errmsg_);
            return;
        }

        fprintf(stdout,"client(%p) ConnectThread end, connect status %d\n",pclient, pclient->connectstatus_);
        pclient->run();
    }

    void CTCPClient::ConnectThread6( void* arg )
    {
        CTCPClient *pclient = (CTCPClient*)arg;
        LOGI("客户端("<<pclient<<")Enter Connect Thread.");
        fprintf(stdout,"client(%p) ConnectThread start\n",pclient);
        struct sockaddr_in6 bind_addr;
        int iret = uv_ip6_addr(pclient->connectip_.c_str(), pclient->connectport_, &bind_addr);
        if (iret) {
            pclient->errmsg_ = GetUVError(iret);
            LOGE(pclient->errmsg_);
            return;
        }

        iret = uv_tcp_connect(&pclient->connect_req_, &pclient->client_, (const sockaddr*)&bind_addr, AfterConnect);
        if (iret) {
            pclient->errmsg_ = GetUVError(iret);
            LOGE(pclient->errmsg_);
            return;
        }

        fprintf(stdout,"client(%p) ConnectThread end, connect status %d\n",pclient, pclient->connectstatus_);
        LOGI("客户端("<<pclient<<")Leave Connect Thread. connect status "<<pclient->connectstatus_);
        pclient->run();
    }

    void CTCPClient::AfterConnect(uv_connect_t* handle, int status)
    {
        fprintf(stdout,"start after connect\n");
        CTCPClient *pclient = (CTCPClient*)handle->handle->data;
        if (status) {
            pclient->connectstatus_ = CONNECT_ERROR;
            fprintf(stdout,"connect error:%s\n",GetUVError(status).c_str());
            return;
        }

        int iret = uv_read_start(handle->handle, onAllocBuffer, AfterClientRecv);//客户端开始接收服务器的数据
        if (iret) {
            fprintf(stdout,"uv_read_start error:%s\n",GetUVError(iret).c_str());
            pclient->connectstatus_ = CONNECT_ERROR;
        } else {
            pclient->connectstatus_ = CONNECT_FINISH;
        }

        LOGI("客户端("<<pclient<<")run");
        fprintf(stdout,"end after connect\n");
    }

    //客户端的发送函数
    int CTCPClient::send(const char* data, std::size_t len)
    {
        //自己控制data的生命周期直到write结束
        if (!data || len <= 0) {
            errmsg_ = "send data is null or len less than zero.";
            return 0;
        }

        uv_mutex_lock(&write_mutex_handle_);
        if (writebuffer_.len < len) {
            writebuffer_.base = (char*)realloc(writebuffer_.base,len);
            writebuffer_.len = len;
        }

        memcpy(writebuffer_.base,data,len);
        uv_buf_t buf = uv_buf_init((char*)writebuffer_.base,len);
        int iret = uv_write(&write_req_, (uv_stream_t*)&client_, &buf, 1, AfterSend);
        if (iret) {
            uv_mutex_unlock(&write_mutex_handle_);
            errmsg_ = GetUVError(iret);
            LOGE(errmsg_);
            return false;
        }
        return true;
    }

    //客户端-接收数据回调函数
    void CTCPClient::setrecvcb(client_recvcb cb, void* userdata )
    {
        recvcb_ = cb;
        userdata_ = userdata;
    }

    //客户端分析空间函数
    void CTCPClient::onAllocBuffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
    {
        if (!handle->data) {
            return;
        }

        CTCPClient *client = (CTCPClient*)handle->data;
        *buf = client->readbuffer_;
    }

    void CTCPClient::AfterClientRecv(uv_stream_t *handle, ssize_t nread, const uv_buf_t* buf)
    {
        if (!handle->data) {
            return;
        }

        CTCPClient *client = (CTCPClient*)handle->data;//服务器的recv带的是TCPClient
        if (nread < 0) {
            if (nread == UV_EOF) {
                fprintf(stdout,"服务器(%p)主动断开\n",handle);
                LOGW("服务器主动断开");
            } else if (nread == UV_ECONNRESET) {
                fprintf(stdout,"服务器(%p)异常断开\n",handle);
                LOGW("服务器异常断开");
            } else {
                fprintf(stdout,"服务器(%p)异常断开:%s\n",handle,GetUVError(nread).c_str());
                LOGW("服务器异常断开"<<GetUVError(nread));
            }

            uv_close((uv_handle_t*)handle, AfterClose);
            return;
        }

        if (nread > 0 && client->recvcb_) {
            client->recvcb_(buf->base,nread,client->userdata_);
        }
    }

    //服务器与客户端一致
    void CTCPClient::AfterSend(uv_write_t *req, int status)
    {
        CTCPClient *client = (CTCPClient *)req->handle->data;
        uv_mutex_unlock(&client->write_mutex_handle_);
        if (status < 0) {
            LOGE("发送数据有误:"<<GetUVError(status));
            fprintf(stderr, "Write error %s\n", GetUVError(status).c_str());
        }
    }

    //服务器与客户端一致
    void CTCPClient::AfterClose(uv_handle_t *handle)
    {
        fprintf(stdout,"客户端(%p)已close\n",handle);
        LOGI("客户端("<<handle<<")已close");
    }

    void CTCPClient::StartLog( const char* logpath /*= nullptr*/ )
    {
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerMonthdir(LOG4Z_MAIN_LOGGER_ID, true);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerDisplay(LOG4Z_MAIN_LOGGER_ID,false);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerLevel(LOG4Z_MAIN_LOGGER_ID,LOG_LEVEL_DEBUG);
        zsummer::log4z::ILog4zManager::getInstance()->setLoggerLimitsize(LOG4Z_MAIN_LOGGER_ID,100);
        if (logpath) {
            zsummer::log4z::ILog4zManager::getInstance()->setLoggerPath(LOG4Z_MAIN_LOGGER_ID,logpath);
        }

        zsummer::log4z::ILog4zManager::getInstance()->start();
    }
}
