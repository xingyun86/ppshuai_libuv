#include <iostream>
using namespace std;
#include <stdlib.h>
#include <uvsocket.h>

int main()
{
    cout << "Hello world!" << endl;
    uvsocket::CTCPClient client;
    client.connect("127.0.0.1", 12716);
    client.send("123", 3);
    client.close();

    return 0;
}
