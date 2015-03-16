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

/* This list file is automatically generated,you shoud NOT modify this file anyway! test comment*/
#ifndef dpsTRACE_H__
#define dpsTRACE_H__
#define SDB__DPSMSGBLK_EXTEND                              0x1000000333L
#define SDB__DPSRPCMGR_INIT                                0x1000000334L
#define SDB__DPSRPCMGR__RESTRORE                           0x1000000335L
#define SDB__DPSRPCMGR_PREPAGES                            0x1000000336L
#define SDB__DPSRPCMGR_WRITEDATA                           0x1000000337L
#define SDB__DPSRPCMGR_MERGE                               0x1000000338L
#define SDB__DPSRPCMGR_GETLSNWIN                           0x1000000339L
#define SDB__DPSRPCMGR_GETLSNWIN2                          0x100000033aL
#define SDB__DPSRPCMGR__MVPAGES                            0x100000033bL
#define SDB__DPSRPCMGR_MOVE                                0x100000033cL
#define SDB__DPSRPCMGR__GETSTARTLSN                        0x100000033dL
#define SDB__DPSRPCMGR__SEARCH                             0x100000033eL
#define SDB__DPSRPCMGR__PARSE                              0x100000033fL
#define SDB__DPSRPCMGR_SEARCH                              0x1000000340L
#define SDB__DPSRPCMGR__ALLOCATE                           0x1000000341L
#define SDB__DPSRPCMGR__PSH2SNDQUEUE                       0x1000000342L
#define SDB__DPSRPCMGR__MRGLOGS                            0x1000000343L
#define SDB__DPSRPCMGR__MRGPAGE                            0x1000000344L
#define SDB__DPSRPCMGR_RUN                                 0x1000000345L
#define SDB__DPSRPCMGR__FLUSHALL                           0x1000000346L
#define SDB__DPSRPCMGR_TEARDOWN                            0x1000000347L
#define SDB__DPSRPCMGR__FLUSHPAGE                          0x1000000348L
#define SDB_DPSTRANSLOCK_ACQUIREX                          0x1000000349L
#define SDB_DPSTRANSLOCK_ACQUIRES                          0x100000034aL
#define SDB_DPSTRANSLOCK_ACQUIREIX                         0x100000034bL
#define SDB_DPSTRANSLOCK_ACQUIREIS                         0x100000034cL
#define SDB_DPSTRANSLOCK_RELEASE                           0x100000034dL
#define SDB_DPSTRANSLOCK_RELEASEALL                        0x100000034eL
#define SDB_DPSTRANSLOCK_UPGRADE                           0x100000034fL
#define SDB_DPSTRANSLOCK_UPGRADECHECK                      0x1000000350L
#define SDB_DPSTRANSLOCK_GETBUCKET                         0x1000000351L
#define SDB_DPSTRANSLOCK_TESTS                             0x1000000352L
#define SDB_DPSTRANSLOCK_TESTUPGRADE                       0x1000000353L
#define SDB_DPSTRANSLOCK_TESTIS                            0x1000000354L
#define SDB_DPSTRANSLOCK_TESTX                             0x1000000355L
#define SDB_DPSTRANSLOCK_TESTIX                            0x1000000356L
#define SDB_DPSTRANSLOCK_TRYX                              0x1000000357L
#define SDB_DPSTRANSLOCK_TRYS                              0x1000000358L
#define SDB_DPSTRANSLOCK_TRYIX                             0x1000000359L
#define SDB_DPSTRANSLOCK_TRYIS                             0x100000035aL
#define SDB_DPSTRANSLOCK_TRYUPGRADE                        0x100000035bL
#define SDB_DPSTRANSLOCK_TRYUPGRADEORAPPEND                0x100000035cL
#define SDB_DPSTRANSLOCK_TRYORAPPENDX                      0x100000035dL
#define SDB_DPSTRANSLOCK_WAIT                              0x100000035eL
#define SDB_DPSTRANSLOCK_HASWAIT                           0x100000035fL
#define SDB__DPSLGFILEMGR_INIT                             0x1000000360L
#define SB__DPSLGFILEMGR__ANLYS                            0x1000000361L
#define SDB__DPSLGFILEMGR_FLUSH                            0x1000000362L
#define SDB__DPSLGFILEMGR_LOAD                             0x1000000363L
#define SDB__DPSLGFILEMGR_MOVE                             0x1000000364L
#define SDB__DPSMGBLK_CLEAR                                0x1000000365L
#define SDB__DPSLGPAGE                                     0x1000000366L
#define SDB__DPSLGPAGE2                                    0x1000000367L
#define SDB__DPSLGRECD_LOAD                                0x1000000368L
#define SDB__DPSLGRECD_FIND                                0x1000000369L
#define SDB__DPSLGRECD_PUSH                                0x100000036aL
#define SDB__DPSLGRECD_DUMP                                0x100000036bL
#define SDB_DPSTRANSCB_SVTRANSINFO                         0x100000036cL
#define SDB_DPSTRANSCB_ADDTRANSCB                          0x100000036dL
#define SDB_DPSTRANSCB_DELTRANSCB                          0x100000036eL
#define SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG                0x100000036fL
#define SDB_DPSTRANSCB_TERMALLTRANS                        0x1000000370L
#define SDB_DPSLOCKBUCKET_ACQUIRE                          0x1000000371L
#define SDB_DPSLOCKBUCKET_WAITLOCKX                        0x1000000372L
#define SDB_DPSLOCKBUCKET_UPGRADE                          0x1000000373L
#define SDB_DPSLOCKBUCKET_LOCKID                           0x1000000374L
#define SDB_DPSLOCKBUCKET_APPENDTORUN                      0x1000000375L
#define SDB_DPSLOCKBUCKET_APPENDTOWAIT                     0x1000000376L
#define SDB_DPSLOCKBUCKET_APPENDHEADTOWAIT                 0x1000000377L
#define SDB_DPSLOCKBUCKET_REMOVEFROMRUN                    0x1000000378L
#define SDB_DPSLOCKBUCKET_REMOVEFROMWAIT                   0x1000000379L
#define SDB_DPSLOCKBUCKET_WAITLOCK                         0x100000037aL
#define SDB_DPSLOCKBUCKET_WAKEUP                           0x100000037bL
#define SDB_DPSLOCKBUCKET_CHECKCOMPATIBLE                  0x100000037cL
#define SDB_DPSLOCKBUCKET_TEST                             0x100000037dL
#define SDB_DPSLOCKBUCKET_TRYACQUIRE                       0x100000037eL
#define SDB_DPSLOCKBUCKET_TRYACQUIREORAPPEND               0x100000037fL
#define SDB_DPSLOCKBUCKET_HASWAIT                          0x1000000380L
#define SDB__DPSLGWRAPP_RECDROW                            0x1000000381L
#define SDB__DPSLGWRAPP_PREPARE                            0x1000000382L
#define SDB__DPSLOGFILE_INIT                               0x1000000383L
#define SDB__DPSLOGFILE__RESTRORE                          0x1000000384L
#define SDB__DPSLOGFILE_RESET                              0x1000000385L
#define SDB__DPSLOGFILE__FLUSHHD                           0x1000000386L
#define SDB__DPSLOGFILE__RDHD                              0x1000000387L
#define SDB__DPSLOGFILE_WRITE                              0x1000000388L
#define SDB__DPSLOGFILE_READ                               0x1000000389L
#define SDB__DPS_INSERT2RECORD                             0x100000038aL
#define SDB_DPS_INSERT2RECORD                              0x100000038bL
#define SDB__DPS_UPDATE2RECORD                             0x100000038cL
#define SDB__DPS_RECORD2UPDATE                             0x100000038dL
#define SDB__DPS_DELETE2RECORD                             0x100000038eL
#define SDB__DPS_RECORD2DELETE                             0x100000038fL
#define SDB__DPS_CSCRT2RECORD                              0x1000000390L
#define SDB__DPS_RECORD2CSCRT                              0x1000000391L
#define SDB__DPS_CSDEL2RECORD                              0x1000000392L
#define SDB__DPS_RECORD2CSDEL                              0x1000000393L
#define SDB__DPS_CLCRT2RECORD                              0x1000000394L
#define SDB__DPS_RECORD2CLCRT                              0x1000000395L
#define SDB__DPS_CLDEL2RECORD                              0x1000000396L
#define SDB__DPS_RECORD2CLDEL                              0x1000000397L
#define SDB__DPS_IXCRT2RECORD                              0x1000000398L
#define SDB__DPS_RECORD2IXCRT                              0x1000000399L
#define SDB__DPS_IXDEL2RECORD                              0x100000039aL
#define SDB__DPS_RECORD2IXDEL                              0x100000039bL
#define SDB__DPS_CLRENAME2RECORD                           0x100000039cL
#define SDB__DPS_RECORD2CLRENAME                           0x100000039dL
#define SDB__DPS_CLTRUNC2RECORD                            0x100000039eL
#define SDB__DPS_RECORD2CLTRUNC                            0x100000039fL
#define SDB__DPS_RECORD2TRANSCOMMIT                        0x10000003a0L
#define SDB__DPS_TRANSCOMMIT2RECORD                        0x10000003a1L
#define SDB__DPS_TRANSROLLBACK2RECORD                      0x10000003a2L
#define SDB__DPS_INVALIDCATA2RECORD                        0x10000003a3L
#define SDB__DPS_RECORD2INVALIDCATA                        0x10000003a4L
#define SDB__DPS_LOBW2RECORD                               0x10000003a5L
#define SDB__DPS_RECORD2LOBW                               0x10000003a6L
#define SDB__DPS_LOBU2RECORD                               0x10000003a7L
#define SDB__DPS_RECORD2LOBU                               0x10000003a8L
#define SDB__DPS_LOBRM2RECORD                              0x10000003a9L
#define SDB__DPS_RECORD2LOBRM                              0x10000003aaL
#define SDB__DPS_LOBTRUNCATE2RECORD                        0x10000003abL
#define SDB__DPS_RECORD2LOBTRUNCATE                        0x10000003acL
#endif
