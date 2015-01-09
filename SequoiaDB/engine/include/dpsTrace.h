/*    Copyright 2012 SequoiaDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
/*    Copyright (C) 2011-2014 SequoiaDB Ltd.
 *    This program is free software: you can redistribute it and/or modify
 *    it under the term of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warrenty of
 *    MARCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/license/>.
 */

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef dpsTRACE_H__
#define dpsTRACE_H__
#define SDB__DPSLGRECD_LOAD                                0x10000004d5L
#define SDB__DPSLGRECD_FIND                                0x10000004d6L
#define SDB__DPSLGRECD_PUSH                                0x10000004d7L
#define SDB__DPSLGRECD_DUMP                                0x10000004d8L
#define SDB__DPSMSGBLK_EXTEND                              0x10000004d9L
#define SDB__DPSLGWRAPP_RECDROW                            0x10000004daL
#define SDB__DPSLGWRAPP_PREPARE                            0x10000004dbL
#define SDB__DPSLGPAGE                                     0x10000004dcL
#define SDB__DPSLGPAGE2                                    0x10000004ddL
#define SDB__DPSLGFILEMGR_INIT                             0x10000004deL
#define SB__DPSLGFILEMGR__ANLYS                            0x10000004dfL
#define SDB__DPSLGFILEMGR_FLUSH                            0x10000004e0L
#define SDB__DPSLGFILEMGR_LOAD                             0x10000004e1L
#define SDB__DPSLGFILEMGR_MOVE                             0x10000004e2L
#define SDB_DPSTRANSLOCK_ACQUIREX                          0x10000004e3L
#define SDB_DPSTRANSLOCK_ACQUIRES                          0x10000004e4L
#define SDB_DPSTRANSLOCK_ACQUIREIX                         0x10000004e5L
#define SDB_DPSTRANSLOCK_ACQUIREIS                         0x10000004e6L
#define SDB_DPSTRANSLOCK_RELEASE                           0x10000004e7L
#define SDB_DPSTRANSLOCK_RELEASEALL                        0x10000004e8L
#define SDB_DPSTRANSLOCK_UPGRADE                           0x10000004e9L
#define SDB_DPSTRANSLOCK_UPGRADECHECK                      0x10000004eaL
#define SDB_DPSTRANSLOCK_GETBUCKET                         0x10000004ebL
#define SDB_DPSTRANSLOCK_TESTS                             0x10000004ecL
#define SDB_DPSTRANSLOCK_TESTUPGRADE                       0x10000004edL
#define SDB_DPSTRANSLOCK_TESTIS                            0x10000004eeL
#define SDB_DPSTRANSLOCK_TESTX                             0x10000004efL
#define SDB_DPSTRANSLOCK_TESTIX                            0x10000004f0L
#define SDB_DPSTRANSLOCK_TRYX                              0x10000004f1L
#define SDB_DPSTRANSLOCK_TRYS                              0x10000004f2L
#define SDB_DPSTRANSLOCK_TRYIX                             0x10000004f3L
#define SDB_DPSTRANSLOCK_TRYIS                             0x10000004f4L
#define SDB_DPSTRANSLOCK_TRYUPGRADE                        0x10000004f5L
#define SDB_DPSTRANSLOCK_TRYUPGRADEORAPPEND                0x10000004f6L
#define SDB_DPSTRANSLOCK_TRYORAPPENDX                      0x10000004f7L
#define SDB_DPSTRANSLOCK_WAIT                              0x10000004f8L
#define SDB_DPSTRANSLOCK_HASWAIT                           0x10000004f9L
#define SDB__DPSRPCMGR_INIT                                0x10000004faL
#define SDB__DPSRPCMGR__RESTRORE                           0x10000004fbL
#define SDB__DPSRPCMGR_PREPAGES                            0x10000004fcL
#define SDB__DPSRPCMGR_WRITEDATA                           0x10000004fdL
#define SDB__DPSRPCMGR_MERGE                               0x10000004feL
#define SDB__DPSRPCMGR_GETLSNWIN                           0x10000004ffL
#define SDB__DPSRPCMGR_GETLSNWIN2                          0x1000000500L
#define SDB__DPSRPCMGR__MVPAGES                            0x1000000501L
#define SDB__DPSRPCMGR_MOVE                                0x1000000502L
#define SDB__DPSRPCMGR__GETSTARTLSN                        0x1000000503L
#define SDB__DPSRPCMGR__SEARCH                             0x1000000504L
#define SDB__DPSRPCMGR__PARSE                              0x1000000505L
#define SDB__DPSRPCMGR_SEARCH                              0x1000000506L
#define SDB__DPSRPCMGR__ALLOCATE                           0x1000000507L
#define SDB__DPSRPCMGR__PSH2SNDQUEUE                       0x1000000508L
#define SDB__DPSRPCMGR__MRGLOGS                            0x1000000509L
#define SDB__DPSRPCMGR__MRGPAGE                            0x100000050aL
#define SDB__DPSRPCMGR_RUN                                 0x100000050bL
#define SDB__DPSRPCMGR__FLUSHALL                           0x100000050cL
#define SDB__DPSRPCMGR_TEARDOWN                            0x100000050dL
#define SDB__DPSRPCMGR__FLUSHPAGE                          0x100000050eL
#define SDB_DPSTRANSCB_SVTRANSINFO                         0x100000050fL
#define SDB_DPSTRANSCB_ADDTRANSCB                          0x1000000510L
#define SDB_DPSTRANSCB_DELTRANSCB                          0x1000000511L
#define SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG                0x1000000512L
#define SDB_DPSTRANSCB_TERMALLTRANS                        0x1000000513L
#define SDB__DPSLOGFILE_INIT                               0x1000000514L
#define SDB__DPSLOGFILE__RESTRORE                          0x1000000515L
#define SDB__DPSLOGFILE_RESET                              0x1000000516L
#define SDB__DPSLOGFILE__FLUSHHD                           0x1000000517L
#define SDB__DPSLOGFILE__RDHD                              0x1000000518L
#define SDB__DPSLOGFILE_WRITE                              0x1000000519L
#define SDB__DPSLOGFILE_READ                               0x100000051aL
#define SDB__DPSMGBLK_CLEAR                                0x100000051bL
#define SDB_DPSLOCKBUCKET_ACQUIRE                          0x100000051cL
#define SDB_DPSLOCKBUCKET_WAITLOCKX                        0x100000051dL
#define SDB_DPSLOCKBUCKET_UPGRADE                          0x100000051eL
#define SDB_DPSLOCKBUCKET_LOCKID                           0x100000051fL
#define SDB_DPSLOCKBUCKET_APPENDTORUN                      0x1000000520L
#define SDB_DPSLOCKBUCKET_APPENDTOWAIT                     0x1000000521L
#define SDB_DPSLOCKBUCKET_APPENDHEADTOWAIT                 0x1000000522L
#define SDB_DPSLOCKBUCKET_REMOVEFROMRUN                    0x1000000523L
#define SDB_DPSLOCKBUCKET_REMOVEFROMWAIT                   0x1000000524L
#define SDB_DPSLOCKBUCKET_WAITLOCK                         0x1000000525L
#define SDB_DPSLOCKBUCKET_WAKEUP                           0x1000000526L
#define SDB_DPSLOCKBUCKET_CHECKCOMPATIBLE                  0x1000000527L
#define SDB_DPSLOCKBUCKET_TEST                             0x1000000528L
#define SDB_DPSLOCKBUCKET_TRYACQUIRE                       0x1000000529L
#define SDB_DPSLOCKBUCKET_TRYACQUIREORAPPEND               0x100000052aL
#define SDB_DPSLOCKBUCKET_HASWAIT                          0x100000052bL
#define SDB__DPS_INSERT2RECORD                             0x100000052cL
#define SDB_DPS_INSERT2RECORD                              0x100000052dL
#define SDB__DPS_UPDATE2RECORD                             0x100000052eL
#define SDB__DPS_RECORD2UPDATE                             0x100000052fL
#define SDB__DPS_DELETE2RECORD                             0x1000000530L
#define SDB__DPS_RECORD2DELETE                             0x1000000531L
#define SDB__DPS_CSCRT2RECORD                              0x1000000532L
#define SDB__DPS_RECORD2CSCRT                              0x1000000533L
#define SDB__DPS_CSDEL2RECORD                              0x1000000534L
#define SDB__DPS_RECORD2CSDEL                              0x1000000535L
#define SDB__DPS_CLCRT2RECORD                              0x1000000536L
#define SDB__DPS_RECORD2CLCRT                              0x1000000537L
#define SDB__DPS_CLDEL2RECORD                              0x1000000538L
#define SDB__DPS_RECORD2CLDEL                              0x1000000539L
#define SDB__DPS_IXCRT2RECORD                              0x100000053aL
#define SDB__DPS_RECORD2IXCRT                              0x100000053bL
#define SDB__DPS_IXDEL2RECORD                              0x100000053cL
#define SDB__DPS_RECORD2IXDEL                              0x100000053dL
#define SDB__DPS_CLRENAME2RECORD                           0x100000053eL
#define SDB__DPS_RECORD2CLRENAME                           0x100000053fL
#define SDB__DPS_CLTRUNC2RECORD                            0x1000000540L
#define SDB__DPS_RECORD2CLTRUNC                            0x1000000541L
#define SDB__DPS_RECORD2TRANSCOMMIT                        0x1000000542L
#define SDB__DPS_TRANSCOMMIT2RECORD                        0x1000000543L
#define SDB__DPS_TRANSROLLBACK2RECORD                      0x1000000544L
#define SDB__DPS_INVALIDCATA2RECORD                        0x1000000545L
#define SDB__DPS_RECORD2INVALIDCATA                        0x1000000546L
#define SDB__DPS_LOBW2RECORD                               0x1000000547L
#define SDB__DPS_RECORD2LOBW                               0x1000000548L
#define SDB__DPS_LOBU2RECORD                               0x1000000549L
#define SDB__DPS_RECORD2LOBU                               0x100000054aL
#define SDB__DPS_LOBRM2RECORD                              0x100000054bL
#define SDB__DPS_RECORD2LOBRM                              0x100000054cL
#define SDB__DPS_LOBTRUNCATE2RECORD                        0x100000054dL
#define SDB__DPS_RECORD2LOBTRUNCATE                        0x100000054eL
#endif
