#include <iostream>
using namespace std;
#include <stdlib.h>
#include <uvsocket.h>

int main()
{
    cout << "Hello world!" << endl;
    uvsocket::CTCPServer server;
    server.Start("0.0.0.0", 12716);
	
    server.close();

    return 0;
}
