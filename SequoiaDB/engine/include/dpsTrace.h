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
#define SDB_DPSTRANSCB_SVTRANSINFO                         0x10000002fdL
#define SDB_DPSTRANSCB_ADDTRANSCB                          0x10000002feL
#define SDB_DPSTRANSCB_DELTRANSCB                          0x10000002ffL
#define SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG                0x1000000300L
#define SDB_DPSTRANSCB_TERMALLTRANS                        0x1000000301L
#define SDB__DPSMGBLK_CLEAR                                0x1000000302L
#define SDB_DPSTRANSLOCK_ACQUIREX                          0x1000000303L
#define SDB_DPSTRANSLOCK_ACQUIRES                          0x1000000304L
#define SDB_DPSTRANSLOCK_ACQUIREIX                         0x1000000305L
#define SDB_DPSTRANSLOCK_ACQUIREIS                         0x1000000306L
#define SDB_DPSTRANSLOCK_RELEASE                           0x1000000307L
#define SDB_DPSTRANSLOCK_RELEASEALL                        0x1000000308L
#define SDB_DPSTRANSLOCK_UPGRADE                           0x1000000309L
#define SDB_DPSTRANSLOCK_UPGRADECHECK                      0x100000030aL
#define SDB_DPSTRANSLOCK_GETBUCKET                         0x100000030bL
#define SDB_DPSTRANSLOCK_TESTS                             0x100000030cL
#define SDB_DPSTRANSLOCK_TESTUPGRADE                       0x100000030dL
#define SDB_DPSTRANSLOCK_TESTIS                            0x100000030eL
#define SDB_DPSTRANSLOCK_TESTX                             0x100000030fL
#define SDB_DPSTRANSLOCK_TESTIX                            0x1000000310L
#define SDB_DPSTRANSLOCK_TRYX                              0x1000000311L
#define SDB_DPSTRANSLOCK_TRYS                              0x1000000312L
#define SDB_DPSTRANSLOCK_TRYIX                             0x1000000313L
#define SDB_DPSTRANSLOCK_TRYIS                             0x1000000314L
#define SDB_DPSTRANSLOCK_TRYUPGRADE                        0x1000000315L
#define SDB_DPSTRANSLOCK_TRYUPGRADEORAPPEND                0x1000000316L
#define SDB_DPSTRANSLOCK_TRYORAPPENDX                      0x1000000317L
#define SDB_DPSTRANSLOCK_WAIT                              0x1000000318L
#define SDB_DPSTRANSLOCK_HASWAIT                           0x1000000319L
#define SDB__DPSLOGFILE_INIT                               0x100000031aL
#define SDB__DPSLOGFILE__RESTRORE                          0x100000031bL
#define SDB__DPSLOGFILE_RESET                              0x100000031cL
#define SDB__DPSLOGFILE__FLUSHHD                           0x100000031dL
#define SDB__DPSLOGFILE__RDHD                              0x100000031eL
#define SDB__DPSLOGFILE_WRITE                              0x100000031fL
#define SDB__DPSLOGFILE_READ                               0x1000000320L
#define SDB__DPSLGWRAPP_RECDROW                            0x1000000321L
#define SDB__DPSLGWRAPP_PREPARE                            0x1000000322L
#define SDB__DPSLGRECD_LOAD                                0x1000000323L
#define SDB__DPSLGRECD_FIND                                0x1000000324L
#define SDB__DPSLGRECD_PUSH                                0x1000000325L
#define SDB__DPSLGRECD_DUMP                                0x1000000326L
#define SDB__DPSMSGBLK_EXTEND                              0x1000000327L
#define SDB__DPSLGPAGE                                     0x1000000328L
#define SDB__DPSLGPAGE2                                    0x1000000329L
#define SDB_DPSLOCKBUCKET_ACQUIRE                          0x100000032aL
#define SDB_DPSLOCKBUCKET_WAITLOCKX                        0x100000032bL
#define SDB_DPSLOCKBUCKET_UPGRADE                          0x100000032cL
#define SDB_DPSLOCKBUCKET_LOCKID                           0x100000032dL
#define SDB_DPSLOCKBUCKET_APPENDTORUN                      0x100000032eL
#define SDB_DPSLOCKBUCKET_APPENDTOWAIT                     0x100000032fL
#define SDB_DPSLOCKBUCKET_APPENDHEADTOWAIT                 0x1000000330L
#define SDB_DPSLOCKBUCKET_REMOVEFROMRUN                    0x1000000331L
#define SDB_DPSLOCKBUCKET_REMOVEFROMWAIT                   0x1000000332L
#define SDB_DPSLOCKBUCKET_WAITLOCK                         0x1000000333L
#define SDB_DPSLOCKBUCKET_WAKEUP                           0x1000000334L
#define SDB_DPSLOCKBUCKET_CHECKCOMPATIBLE                  0x1000000335L
#define SDB_DPSLOCKBUCKET_TEST                             0x1000000336L
#define SDB_DPSLOCKBUCKET_TRYACQUIRE                       0x1000000337L
#define SDB_DPSLOCKBUCKET_TRYACQUIREORAPPEND               0x1000000338L
#define SDB_DPSLOCKBUCKET_HASWAIT                          0x1000000339L
#define SDB__DPS_INSERT2RECORD                             0x100000033aL
#define SDB_DPS_INSERT2RECORD                              0x100000033bL
#define SDB__DPS_UPDATE2RECORD                             0x100000033cL
#define SDB__DPS_RECORD2UPDATE                             0x100000033dL
#define SDB__DPS_DELETE2RECORD                             0x100000033eL
#define SDB__DPS_RECORD2DELETE                             0x100000033fL
#define SDB__DPS_CSCRT2RECORD                              0x1000000340L
#define SDB__DPS_RECORD2CSCRT                              0x1000000341L
#define SDB__DPS_CSDEL2RECORD                              0x1000000342L
#define SDB__DPS_RECORD2CSDEL                              0x1000000343L
#define SDB__DPS_CLCRT2RECORD                              0x1000000344L
#define SDB__DPS_RECORD2CLCRT                              0x1000000345L
#define SDB__DPS_CLDEL2RECORD                              0x1000000346L
#define SDB__DPS_RECORD2CLDEL                              0x1000000347L
#define SDB__DPS_IXCRT2RECORD                              0x1000000348L
#define SDB__DPS_RECORD2IXCRT                              0x1000000349L
#define SDB__DPS_IXDEL2RECORD                              0x100000034aL
#define SDB__DPS_RECORD2IXDEL                              0x100000034bL
#define SDB__DPS_CLRENAME2RECORD                           0x100000034cL
#define SDB__DPS_RECORD2CLRENAME                           0x100000034dL
#define SDB__DPS_CLTRUNC2RECORD                            0x100000034eL
#define SDB__DPS_RECORD2CLTRUNC                            0x100000034fL
#define SDB__DPS_RECORD2TRANSCOMMIT                        0x1000000350L
#define SDB__DPS_TRANSCOMMIT2RECORD                        0x1000000351L
#define SDB__DPS_TRANSROLLBACK2RECORD                      0x1000000352L
#define SDB__DPS_INVALIDCATA2RECORD                        0x1000000353L
#define SDB__DPS_RECORD2INVALIDCATA                        0x1000000354L
#define SDB__DPS_LOBW2RECORD                               0x1000000355L
#define SDB__DPS_RECORD2LOBW                               0x1000000356L
#define SDB__DPS_LOBU2RECORD                               0x1000000357L
#define SDB__DPS_RECORD2LOBU                               0x1000000358L
#define SDB__DPS_LOBRM2RECORD                              0x1000000359L
#define SDB__DPS_RECORD2LOBRM                              0x100000035aL
#define SDB__DPS_LOBTRUNCATE2RECORD                        0x100000035bL
#define SDB__DPS_RECORD2LOBTRUNCATE                        0x100000035cL
#define SDB__DPSLGFILEMGR_INIT                             0x100000035dL
#define SB__DPSLGFILEMGR__ANLYS                            0x100000035eL
#define SDB__DPSLGFILEMGR_FLUSH                            0x100000035fL
#define SDB__DPSLGFILEMGR_LOAD                             0x1000000360L
#define SDB__DPSLGFILEMGR_MOVE                             0x1000000361L
#define SDB__DPSRPCMGR_INIT                                0x1000000362L
#define SDB__DPSRPCMGR__RESTRORE                           0x1000000363L
#define SDB__DPSRPCMGR_PREPAGES                            0x1000000364L
#define SDB__DPSRPCMGR_WRITEDATA                           0x1000000365L
#define SDB__DPSRPCMGR_MERGE                               0x1000000366L
#define SDB__DPSRPCMGR_GETLSNWIN                           0x1000000367L
#define SDB__DPSRPCMGR_GETLSNWIN2                          0x1000000368L
#define SDB__DPSRPCMGR__MVPAGES                            0x1000000369L
#define SDB__DPSRPCMGR_MOVE                                0x100000036aL
#define SDB__DPSRPCMGR__GETSTARTLSN                        0x100000036bL
#define SDB__DPSRPCMGR__SEARCH                             0x100000036cL
#define SDB__DPSRPCMGR__PARSE                              0x100000036dL
#define SDB__DPSRPCMGR_SEARCH                              0x100000036eL
#define SDB__DPSRPCMGR__ALLOCATE                           0x100000036fL
#define SDB__DPSRPCMGR__PSH2SNDQUEUE                       0x1000000370L
#define SDB__DPSRPCMGR__MRGLOGS                            0x1000000371L
#define SDB__DPSRPCMGR__MRGPAGE                            0x1000000372L
#define SDB__DPSRPCMGR_RUN                                 0x1000000373L
#define SDB__DPSRPCMGR__FLUSHALL                           0x1000000374L
#define SDB__DPSRPCMGR_TEARDOWN                            0x1000000375L
#define SDB__DPSRPCMGR__FLUSHPAGE                          0x1000000376L
#endif
