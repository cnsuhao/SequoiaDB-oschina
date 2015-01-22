#ifndef _SDB_MONGO_MSG_CONVERTER_HPP_
#define _SDB_MONGO_MSG_CONVERTER_HPP_

#include "util.hpp"
#include "dpsLogWrapper.hpp"
#include "rtnContextBuff.hpp"
#include "pmdSession.hpp"

class engine::_IProcessor ;
class mongoConverter ;

class SDB_EXPORT _pmdMongoSession : public engine::pmdSession
{
public:
   _pmdMongoSession( SOCKET fd ) ;
   virtual ~_pmdMongoSession() ;

   virtual UINT64 identifyID() ;
   virtual INT32 getServiceType() const ;
   virtual engine::SDB_SESSION_TYPE sessionType() const ;
   INT32 attachProcessor( engine::_IProcessor *processor ) ;
   void detachProcessor() ;

   virtual INT32 run() ;

protected:
   INT32 _processMsg( const CHAR *pMsg, const INT32 len ) ;
   INT32 _onMsgBegin( MsgHeader *msg ) ;
   INT32 _onMsgEnd( INT32 result, MsgHeader *msg ) ;
   INT32 _reply( MsgOpReply *replyHeader, const CHAR *pBody, const INT32 len ) ;
   virtual void  _onAttach() ;
   virtual void  _onDetach() ;

private:
   void  _zeroStream() ;

private:
   engine::_dpsLogWrapper *_pDPSCB ;
   engine::_IProcessor    *_processor ;
   mongoConverter         *_converter ;
   MsgOpReply              _replyHeader ;
   BOOLEAN                 _needReply ;
   engine::rtnContextBuf   _contextBuff ;
   BSONObj                 _errorInfo ;

   fixedStream             _inStream ;
   fixedStream             _outStream ;
} ;

typedef _pmdMongoSession pmdMongoSession ;

#endif
