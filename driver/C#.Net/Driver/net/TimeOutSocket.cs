using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace SequoiaDB
{
    class TimeOutSocket
    {
        private static ManualResetEvent timeoutObject = new ManualResetEvent(false);
        private static bool connected = false;
        private static Exception socketException;

        internal static TcpClient Connect(IPEndPoint remoteEndPoint, int timeoutMSec)
        {
            timeoutObject.Reset();
            socketException = null;

            string servIP = Convert.ToString(remoteEndPoint.Address);
            int servPort = remoteEndPoint.Port;
            TcpClient client = new TcpClient();
            try
            {
                client.BeginConnect(servIP, servPort, new AsyncCallback(CallBackMethod), client);
            }
            catch (Exception)
            {
                throw new BaseException("SDB_NET_CANNOT_CONNECT");
            }

            if (timeoutObject.WaitOne(timeoutMSec, false))
            {
                if (connected)
                    return client;
                else
                    throw socketException;
            }
            else
            {
                client.Close();
                throw new BaseException("SDB_TIMEOUT");
            }
        }

        private static void CallBackMethod(IAsyncResult asyncResult)
        {
            try
            {
                connected = false;
                TcpClient client = asyncResult.AsyncState as TcpClient;
                if (client.Client != null)
                {
                    client.EndConnect(asyncResult);
                    connected = true;
                }
            }
            catch (Exception)
            {
                connected = false;
                socketException = new BaseException("SDB_NET_CANNOT_CONNECT");
            }
            finally
            {
                timeoutObject.Set();
            }
        }
    }
}
