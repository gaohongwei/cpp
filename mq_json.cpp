#include <stdio.h>
#include <string>
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include "mq.h"

using namespace std;
using boost::property_tree::ptree;
using namespace mq;

class WebMsg
{
private:
    ptree m_data;
    ptree m_msg;
public:
    WebMsg()
    {
        m_msg.put("class", "Rester");
    }
    static string pt_to_s(ptree pt)
    {
        std::stringstream so;
        write_json(so, pt);
        return so.str();
    }
    string to_s()
    {
        m_msg.put("args",pt_to_s(m_data));
        return pt_to_s(m_msg);
    }
    string to_s(string msg)
    {
        m_msg.put("args",msg);
        return pt_to_s(m_msg);
    }
    void set(string key,string value){
       m_data.put(key,value);
    }

    void clean(){

    }
};

class WebMSQ
{
private:
    Connection *mp_conn;
    string m_mq;
public:
    WebMSQ(string mq="queue:rester",string ip="127.0.0.1",string port="6379"):mp_conn(new Connection(ip,port,"")),m_mq(mq){}
    ~WebMSQ(){delete mp_conn;}
    void print()
    {
        int length = mp_conn->llen(m_mq);
        printf("\nMQ %s:\n", m_mq.c_str());
        MultiBulkEnumerator result = mp_conn->lrange(m_mq, 0, length);

        string data;
        while (result.next(&data)) {
            printf("%s\n", data.c_str());
        }
        printf("\n");
        printf("\nMQ %s has %d messages.\n", m_mq.c_str(),length);
    }
    IntReply send(string message)
    {
        return mp_conn->rpush(m_mq, message);
    }
    IntReply send(string message)
    {
        return mp_conn->lpop(m_mq);
    }
};

int main(int argc, char* argv[])
{
    if(argc < 2) {
        printf("\nUsage: test_me message\n");
        return 0;
    }
    string message=argv[1];
    WebMSQ mq_out("rester");
    WebMSQ mq_in("updater");
    WebMsg msg;
    msg.set("table","hsm");
    msg.set("hsmID","hsm01");
    cout << msg.to_s();
    //mq_out.send(msg.to_s());
    //mq_out.print();

    cout << msg.to_s(message);
    mq_out.send(msg.to_s(message));
    mq_out.print();
    mq_in.print();
}
