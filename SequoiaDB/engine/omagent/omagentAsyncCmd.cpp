/*******************************************************************************


   Copyright (C) 2011-2014 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the term of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warrenty of
   MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.

   Source File Name = omagentCommand.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/06/2014  TZB Initial Draft

   Last Changed =

*******************************************************************************/

#include "omagentAsyncCmd.hpp"
#include "omagentUtil.hpp"
#include "omagentHelper.hpp"
#include "ossProc.hpp"
#include "utilPath.hpp"
#include "ossPath.h"
#include "omagentJob.hpp"
#include "omagentMgr.hpp"

using namespace bson ;

namespace engine
{
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaAddHost )
/*
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaRemoveHost )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaInsDBBus )
   IMPLEMENT_OACMD_AUTO_REGISTER( _omaUninsDBBus )
*/

   /******************************* add host ********************************/
   /*
      _omaAddHost
   */
   _omaAddHost::_omaAddHost ()
   {
   }

   _omaAddHost::~_omaAddHost ()
   {
      _transactionID = -1 ;
   }

   INT32 _omaAddHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;
         _addHostInfo = bus.getOwned() ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Add hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADD_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADD_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   INT32 _omaAddHost::final ( BSONObj &rval, BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      INT32 retErrno = SDB_OK ;
      BSONObjBuilder bob ;
      BSONObj result ;
      BSONObj addHostResult ;
      BSONObj rollbackInfo ;
      BSONObj rollbackResult ;
      CHAR detail[OMA_BUFF_SIZE + 1] = { 0 } ;

      PD_LOG ( PDDEBUG, "Add hosts return raw result: %s",
               rval.toString(FALSE, TRUE).c_str() ) ;
      rc = omaGetObjElement( rval, "", addHostResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d", "", rc ) ;
         goto error ;
      } 
      rc = omaGetIntElement ( addHostResult, OMA_FIELD_ERRNO, retErrno ) ; 
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d",
                  OMA_FIELD_ERRNO, rc ) ;
         goto error ;
      }
      if ( retErrno )
      {
         PD_LOG ( PDERROR, "Failed to add all the hosts, "
                  "going to rollback, rc = %d", retErrno ) ;
         rc = _getRollbackInfo( addHostResult , rollbackInfo ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to get add hosts roolback info, "
                     "failed to rollback, rc = %d", rc ) ;
            goto error ;
         }
         rc = _addHostRollback( rollbackInfo, rollbackResult ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to roolback add hosts, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         rc = _buildErrDetail( addHostResult, rollbackResult,
                               detail, OMA_BUFF_SIZE ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to build return detail, "
                     "rc = %d", rc ) ;
            goto error ;
         }
         bob.append( OMA_FIELD_DETAIL, detail ) ;
         result = bob.obj() ;
         rc = retErrno ;
         goto done ;
      }

   done:
       _buildRetResult( result, retObj ) ;
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_getRollbackInfo( BSONObj &addHostResult,
                                        BSONObj &rollbackInfo )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      set<string> ips ;

      try
      {
         ele = addHostResult.getField ( OMA_FIELD_HOSTINFO ) ;
         if ( Array != ele.type() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "Js return wrong format result" ) ;
            goto error ;
         }
         else
         {
            BSONArrayBuilder bab ;
            BSONObjIterator itr( ele.embeddedObject() ) ;
            while ( itr.more() )
            {
               BOOLEAN value = FALSE ;
               const CHAR *ip = NULL ;
               ele = itr.next() ;
               if ( Object != ele.type() )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG ( PDERROR, "Js return wrong format result" ) ;
                  goto error ;
               }
               BSONObj temp = ele.embeddedObject() ;
               rc = omaGetBooleanElement( temp, OMA_FIELD_HASINSTALL, value ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                           "rc = %d", OMA_FIELD_HASINSTALL, rc ) ;
                  goto error ;
               }
               if ( value )
               {
                  rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
                  if( rc )
                  {
                     PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                              "rc = %d", OMA_FIELD_IP, rc ) ;
                     goto error ;
                  }
                  ips.insert( string(ip) ) ;
               }
            }
         }
         ele = _addHostInfo.getField ( OMA_FIELD_HOSTINFO ) ;
         if ( Array != ele.type() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG ( PDERROR, "Wrong format input info" ) ;
            goto error ;
         }
         else
         {
            BSONObjBuilder bob ;
            BSONArrayBuilder bab ;
            BSONObjIterator itr( ele.embeddedObject() ) ;
            while ( itr.more() )
            {
               const CHAR *ip = NULL ;
               set<string>::iterator it ;
               ele = itr.next() ;
               if ( Object != ele.type() )
               {
                  rc = SDB_INVALIDARG ;
                  PD_LOG ( PDERROR, "Wrong format input info" ) ;
                  goto error ;
               }
               BSONObj temp = ele.embeddedObject() ;
               rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
               if ( rc )
               {
                  PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                           "rc = %d", OMA_FIELD_IP, rc ) ;
                  goto error ;
               }
               it = ips.find( string(ip) ) ;
               if ( it != ips.end() )
               {
                  bab.append( temp ) ;
               }
            }
            bob.appendArray( OMA_FIELD_HOSTINFO, bab.arr() ) ;
            rollbackInfo = bob.obj() ;
         }
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaAddHost::_addHostRollback ( BSONObj &rollbackInfo,
                                         BSONObj &rollbackResult )
   {
      INT32 rc = SDB_OK ;

      _omaAddHostRollbackInternal rollbackInternal ;
      rc = rollbackInternal.init( rollbackInfo.objdata() ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init rollback in add hosts, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = rollbackInternal.doit( rollbackResult ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to do rollback in add hosts, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      PD_LOG ( PDEVENT, "Rollback add hosts result is: %s",
               rollbackResult.toString(FALSE, TRUE).c_str() ) ;
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_buildErrDetail ( BSONObj &addHostResult,
                                        BSONObj &rollbackResult,
                                        CHAR *pBuf,
                                        INT32 bufSize )
   {
      INT32 rc = SDB_OK ;
      BSONElement ele ;
      BSONArrayBuilder bab ;
      BSONArray arr ;
      const CHAR *pDetail = NULL ;
      string str = "" ;

      rc = omaGetStringElement( addHostResult, OMA_FIELD_DETAIL, &pDetail ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Get field[%s] failed, rc: %d",
                  OMA_FIELD_DETAIL, rc ) ;
         goto error ;
      }
      ele = rollbackResult.getField ( OMA_FIELD_HOSTINFO ) ;
      if ( Array != ele.type() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Js return Wrong format bson" ) ;
         goto error ;
      }
      else
      {
         BSONObjIterator itr( ele.embeddedObject() ) ;
         while ( itr.more() )
         {
            const CHAR *ip = "" ;
            BOOLEAN hasUninstall = FALSE ;
            ele = itr.next() ;
            if ( Object != ele.type() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG ( PDERROR, "Js return Wrong format bson" ) ;
               goto error ;
            }
            BSONObj temp = ele.embeddedObject() ;
            rc = omaGetStringElement( temp, OMA_FIELD_IP, &ip ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                        "rc = %d", OMA_FIELD_IP, rc ) ;
            }
            rc = omaGetBooleanElement( temp, OMA_FIELD_HASUNINSTALL,
                                       hasUninstall ) ;
            if ( rc )
            {
               PD_LOG ( PDERROR, "Failed to get bson field[%s], "
                        "rc = %d", OMA_FIELD_HASUNINSTALL, rc ) ;
            }
            if ( !hasUninstall )
            {
               bab.append ( ip ) ;
            }
         }
         arr = bab.arr() ;
         if ( !arr.isEmpty() )
         {
            str = ", need to uninstall db packet in these hosts manually: " ;
            str += arr.toString( TRUE, FALSE ).c_str() ;
         }
      }
      ossSnprintf( pBuf, OMA_BUFF_SIZE, "%s%s", pDetail, str.c_str() ) ;  
      PD_LOG_MSG ( PDERROR, pBuf ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost::_buildRetResult( BSONObj &obj,
                                       BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObjBuilder bob ;
         bob.append( OMA_FIELD_TRANSACTION_ID, _transactionID ) ;
         bob.appendElements( obj ) ;
         retObj = bob.obj() ; 
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDERROR, "Occur exception: %s", e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   /******************************* add host2 ********************************/
   /*
      _omaAddHost2
   */
   _omaAddHost2::_omaAddHost2 ()
   {
   }

   _omaAddHost2::~_omaAddHost2 ()
   {
   }

   INT32 _omaAddHost2::init( const CHAR *pAddHostInfo )
   {
      INT32 rc = SDB_OK ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start add hosts task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaAddHost2::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   /******************************* remove host ********************************/
   /*
      _omaRemoveHost
   */
   _omaRemoveHost::_omaRemoveHost ()
   {
   }

   _omaRemoveHost::~_omaRemoveHost ()
   {
   }

   INT32 _omaRemoveHost::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_HOST, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_REMOVE_HOST, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error :
      goto done ;
   }

   /******************************* add db business **************************/
   _omaInsDBBus::_omaInsDBBus ()
   {
   }

   _omaInsDBBus::~_omaInsDBBus ()
   {
   }

   INT32 _omaInsDBBus::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start install db business task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaInsDBBus::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   /******************************* uninstall db business *********************/
   _omaUninsDBBus::_omaUninsDBBus ()
   {
   }

   _omaUninsDBBus::~_omaUninsDBBus ()
   {
   }

   INT32 _omaUninsDBBus::init( const CHAR *pUninstallInfo )
   {
      INT32 rc = SDB_OK ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to start remove db business task "
                 "job, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _omaUninsDBBus::doit( BSONObj &retObj )
   {
      return SDB_OK ;
   }

   _omaCreateTmpCoord::_omaCreateTmpCoord ()
   {
   }

   _omaCreateTmpCoord::~_omaCreateTmpCoord ()
   {
   }

   INT32 _omaCreateTmpCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus ;
         if ( NULL == pInstallInfo )
         {
            BSONObjBuilder bob ;
            BSONArrayBuilder bab ;
            bob.appendArray( OMA_FIELD_CATAADDR, bab.arr() ) ;
            bus = bob.obj() ;
         }
         else
         {
            bus = BSONObj(pInstallInfo).getOwned() ;
         }
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Create temporary coord passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_CREATE_TMP_COORD, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_CREATE_TMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaCreateTmpCoord::createTmpCoord( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to create "
                  "temporary coord, rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to create temporary coord, rc = %d", rc ) ;
         goto error ;
      }     
   done:
      return rc ;
   error:
      goto done ;
   }

   _omaRemoveTmpCoord::_omaRemoveTmpCoord ( const CHAR *pTmpCoordSvcName )
   {
      ossStrncpy( _tmpCoordSvcName, pTmpCoordSvcName, OSS_MAX_SERVICENAME ) ;
   }

   _omaRemoveTmpCoord::~_omaRemoveTmpCoord ()
   {
   }

   INT32 _omaRemoveTmpCoord::init ( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj sys = BSON ( OMA_FIELD_TMPCOORDSVCNAME << _tmpCoordSvcName ) ;

         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_SYS, sys.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Remove temporary coord passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_REMOVE_TMP_COORD, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG_MSG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                         FILE_REMOVE_TMP_COORD, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG ( PDERROR, "Failed to build bson, exception is: %s",
                      e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
     goto done ;
   }

   INT32 _omaRemoveTmpCoord::removeTmpCoord( BSONObj &retObj )
   {
      INT32 rc = SDB_OK ;
      rc = init( NULL ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Failed to init to remove temporary coord, "
                  "rc = %d", rc ) ;
         goto error ;
      }
      rc = doit( retObj ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Failed to remove temporary coord, rc = %d", rc ) ;
         goto error ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   _omaAddHostRollbackInternal::_omaAddHostRollbackInternal()
   {
   }

   _omaAddHostRollbackInternal::~_omaAddHostRollbackInternal()
   {
   }

   INT32 _omaAddHostRollbackInternal::init( const CHAR *pInstallInfo )
   {
      INT32 rc = SDB_OK ;
      try
      {
         BSONObj bus( pInstallInfo ) ;
         ossSnprintf( _jsFileArgs, JS_ARG_LEN, "var %s = %s; ",
                      JS_ARG_BUS, bus.toString(FALSE, TRUE).c_str() ) ;
         PD_LOG ( PDDEBUG, "Rollback add hosts passes argument: %s",
                  _jsFileArgs ) ;
         rc = addJsFile( FILE_ADDHOST_ROLLBACK, _jsFileArgs ) ;
         if ( rc )
         {
            PD_LOG ( PDERROR, "Failed to add js file[%s], rc = %d ",
                     FILE_ADDHOST_ROLLBACK, rc ) ;
            goto error ;
         }
      }
      catch ( std::exception &e )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG ( PDERROR, "Failed to build bson, exception is: %s",
                  e.what() ) ;
         goto error ;
      }
   done:
      return rc ;
   error :
      goto done ;
   }


} // namespace engine

