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
#ifndef pmdTRACE_H__
#define pmdTRACE_H__
#define SDB_PMDLOADWORKER                                  0x200000000001fbL
#define SDB_PMDBGJOBENTPNT                                 0x200000000001fcL
#define SDB_PMDLOCALAGENTENTPNT                            0x200000000001fdL
#define SDB_READFROMPIPE                                   0x200000000001feL
#define SDB_MONITOR_THREAD                                 0x200000000001ffL
#define SDB_CREATESHMONTHREAD                              0x20000000000200L
#define SDB_ENTERDAEMONMODE                                0x20000000000201L
#define SDB_SDBBP_MAIN                                     0x20000000000202L
#define SDB_PMDPIPELSTNNPNTPNT                             0x20000000000203L
#define SDB__PMDSTARTUP_INIT                               0x20000000000204L
#define SDB__PMDSTARTUP_FINAL                              0x20000000000205L
#define SDB_PMDLOGGWENTPNT                                 0x20000000000206L
#define SDB_SDBINSPT_RESVARG                               0x20000000000207L
#define SDB_FLUSHOUTPUT                                    0x20000000000208L
#define SDB_DUMPPRINTF                                     0x20000000000209L
#define SDB_REALLOCBUFFER                                  0x2000000000020aL
#define SDB_GETEXTBUFFER                                   0x2000000000020bL
#define SDB_INSPECTHEADER                                  0x2000000000020cL
#define SDB_DUMPHEADER                                     0x2000000000020dL
#define SDB_INSPECTSME                                     0x2000000000020eL
#define SDB_DUMPSME                                        0x2000000000020fL
#define SDB_GETEXTENTHEAD                                  0x20000000000210L
#define SDB_GETEXTENT                                      0x20000000000211L
#define SDB_EXTENTSANITYCHK                                0x20000000000212L
#define SDB_LOADMB                                         0x20000000000213L
#define SDB_LOADEXTENT                                     0x20000000000214L
#define SDB_INSPOVFLWRECRDS                                0x20000000000215L
#define SDB_DUMPOVFWRECRDS                                 0x20000000000216L
#define SDB_INSPINXDEF                                     0x20000000000217L
#define SDB_DUMPINXDEF                                     0x20000000000218L
#define SDB_INSPINXEXTS                                    0x20000000000219L
#define SDB_DUMPINXEXTS                                    0x2000000000021aL
#define SDB_INSPCOLL                                       0x2000000000021bL
#define SDB_DUMPCOLL                                       0x2000000000021cL
#define SDB_INSPCOLLS                                      0x2000000000021dL
#define SDB_DUMPCOLLS                                      0x2000000000021eL
#define SDB_INSPCOLLECTIONS                                0x2000000000021fL
#define SDB_DUMPRAWPAGE                                    0x20000000000220L
#define SDB_DUMPCOLLECTIONS                                0x20000000000221L
#define SDB_ACTIONCSATTEMPT                                0x20000000000222L
#define SDB_DUMPPAGES                                      0x20000000000223L
#define SDB_DUMPDB                                         0x20000000000224L
#define SDB_INSPECTDB                                      0x20000000000225L
#define SDB_SDBINSPT_MAIN                                  0x20000000000226L
#define SDB__PMDTMHD                                       0x20000000000227L
#define SDB__PMDTMHD_DES                                   0x20000000000228L
#define SDB__PMDTMHD_HDTMOUT                               0x20000000000229L
#define SDB__PMDMSGHND                                     0x2000000000022aL
#define SDB__PMDMSGHND_DESC                                0x2000000000022bL
#define SDB__PMDMSGHND_CPMSG                               0x2000000000022cL
#define SDB__PMDMSGHND_HNDMSG                              0x2000000000022dL
#define SDB__PMDMSGHND_HNDCLOSE                            0x2000000000022eL
#define SDB__PMDMSGHND_ONSTOP                              0x2000000000022fL
#define SDB__PMDMSGHND_HNDSNMSG                            0x20000000000230L
#define SDB__PMDMSGHND_HNDMAINMSG                          0x20000000000231L
#define SDB__PMDMSGHND_POSTMAINMSG                         0x20000000000232L
#define SDB_SDBLIST_RESVARG                                0x20000000000233L
#define SDB_SDBLIST_MAIN                                   0x20000000000234L
#define SDB_SDBLOAD_RESOLVEARG                             0x20000000000235L
#define SDB_SDBLOAD_LOADRECV                               0x20000000000236L
#define SDB_SDBLOAD_CONNECTSDB                             0x20000000000237L
#define SDB_SDBLOAD_MAIN                                   0x20000000000238L
#define SDB_PMDCOORDNETWKENTPNT                            0x20000000000239L
#define SDB_PMDTCPLSTNENTPNT                               0x2000000000023aL
#define SDB__PMDOPTMGR_INIT                                0x2000000000023bL
#define SDB__PMDOPTMGR__MKDIR                              0x2000000000023cL
#define SDB__PMDOPTMGR_REFLUSH2FILE                        0x2000000000023dL
#define SDB_PMDSIGHND                                      0x2000000000023eL
#define SDB_PMDEDUUSERTRAPHNDL                             0x2000000000023fL
#define SDB_PMDCTRLHND                                     0x20000000000240L
#define SDB__PMDEDUMGR_DELIOSVC                            0x20000000000241L
#define SDB__PMDEDUMGR_DUMPINFO                            0x20000000000242L
#define SDB__PMDEDUMGR_DUMPINFO2                           0x20000000000243L
#define SDB__PMDEDUMGR_DESTROYALL                          0x20000000000244L
#define SDB__PMDEDUMGR_FORCEUSREDU                         0x20000000000245L
#define SDB__PMDEDUMGR__FORCEIOSVC                         0x20000000000246L
#define SDB__PMDEDUMGR__FORCEEDUS                          0x20000000000247L
#define SDB__PMDEDUMGR__GETEDUCNT                          0x20000000000248L
#define SDB__PMDEDUMGR_PSTEDUPST                           0x20000000000249L
#define SDB__PMDEDUMGR_WAITEDUPST                          0x2000000000024aL
#define SDB__PMDEDUMGR_RTNEDU                              0x2000000000024bL
#define SDB__PMDEDUMGR_STARTEDU                            0x2000000000024cL
#define SDB__PMDEDUMGR_CRTNEWEDU                           0x2000000000024dL
#define SDB__PMDEDUMGR_DSTEDU                              0x2000000000024eL
#define SDB__PMDEDUMGR_WAITEDU                             0x2000000000024fL
#define SDB__PMDEDUMGR_WAITEDU2                            0x20000000000250L
#define SDB__PMDEDUMGR_DEATVEDU                            0x20000000000251L
#define SDB__PMDEDUMGR_ATVEDU                              0x20000000000252L
#define SDB__PMDEDUMGR_ATVEDU2                             0x20000000000253L
#define SDB__PMDEDUMGR_WAITUTIL                            0x20000000000254L
#define SDB__PMDEDUMGR_WAITUTIL2                           0x20000000000255L
#define SDB__PMDEDUMGR_GETEDUTRDID                         0x20000000000256L
#define SDB_READFILE                                       0x20000000000257L
#define SDB_PARSEARGUMENTS                                 0x20000000000258L
#define SDB_ENTERBATCHMODE                                 0x20000000000259L
#define SDB_ENTERINTATVMODE                                0x2000000000025aL
#define SDB_FORMATARGS                                     0x2000000000025bL
#define SDB_CREATEDAEMONPROC                               0x2000000000025cL
#define SDB_ENTERFRONTENDMODE                              0x2000000000025dL
#define SDB_SDB_MAIN                                       0x2000000000025eL
#define SDB_REGEDUNAME                                     0x2000000000025fL
#define SDB__PMDEDUCB_DISCONNECT                           0x20000000000260L
#define SDB__PMDEDUCB_FORCE                                0x20000000000261L
#define SDB__PMDEDUCB_ISINT                                0x20000000000262L
#define SDB__PMDEDUCB_PRINTINFO                            0x20000000000263L
#define SDB__PMDEDUCB_GETINFO                              0x20000000000264L
#define SDB__PMDEDUCB_RESETINFO                            0x20000000000265L
#define SDB__PMDEDUCB_CONTXTPEEK                           0x20000000000266L
#define SDB___PMDEDUCB_DUMPINFO                            0x20000000000267L
#define SDB___PMDEDUCB_DUMPINFO2                           0x20000000000268L
#define SDB__PMDEDUCB_GETTRANSLOCK                         0x20000000000269L
#define SDB__PMDEDUCB_ADDLOCKINFO                          0x2000000000026aL
#define SDB__PMDEDUCB_DELLOCKINFO                          0x2000000000026bL
#define SDB__PMDEDUCB_CREATETRANSACTION                    0x2000000000026cL
#define SDB__PMDEDUCB_DELTRANSACTION                       0x2000000000026dL
#define SDB__PMDEDUCB_ADDTRANSNODE                         0x2000000000026eL
#define SDB__PMDEDUCB_GETTRANSNODEROUTEID                  0x2000000000026fL
#define SDB__PMDEDUCB_ISTRANSNODE                          0x20000000000270L
#define SDB_PMDEDUENTPNT                                   0x20000000000271L
#define SDB_PMDRECV                                        0x20000000000272L
#define SDB_PMDSEND                                        0x20000000000273L
#define SDB_PMDSYNCSESSIONAGENTEP                          0x20000000000274L
#define SDB_SDBSTART_RESVARG                               0x20000000000275L
#define SDB_SDBSTART_MAIN                                  0x20000000000276L
#define SDB_PMDCBMGREP                                     0x20000000000277L
#define SDB__PMDSN                                         0x20000000000278L
#define SDB__PMDSN_DESC                                    0x20000000000279L
#define SDB__PMDSN_ATHIN                                   0x2000000000027aL
#define SDB__PMDSN_ATHOUT                                  0x2000000000027bL
#define SDB__PMDSN_CLEAR                                   0x2000000000027cL
#define SDB__PMDSN__MKNAME                                 0x2000000000027dL
#define SDB__PMDSN__LOCK                                   0x2000000000027eL
#define SDB__PMDSN__UNLOCK                                 0x2000000000027fL
#define SDB__PMDSN_WTATH                                   0x20000000000280L
#define SDB__PMDSN_WTDTH                                   0x20000000000281L
#define SDB__PMDSN_CPMSG                                   0x20000000000282L
#define SDB__PMDSN_FRNBUF                                  0x20000000000283L
#define SDB__PMDSN_POPBUF                                  0x20000000000284L
#define SDB__PMDSN_PSHBUF                                  0x20000000000285L
#define PMD_SESSMGR                                        0x20000000000286L
#define PMD_SESSMGR_DESC                                   0x20000000000287L
#define PMD_SESSMGR_INIT                                   0x20000000000288L
#define PMD_SESSMGR_FINI                                   0x20000000000289L
#define PMD_SESSMGR_FORCENTY                               0x2000000000028aL
#define PMD_SESSMGR_ONTIMER                                0x2000000000028bL
#define PMD_SESSMGR_PUSHMSG                                0x2000000000028cL
#define PMD_SESSMGR_GETSESSION                             0x2000000000028dL
#define CLS_PMDSMGR_ATCHMETA                               0x2000000000028eL
#define PMD_SESSMGR_STARTEDU                               0x2000000000028fL
#define PMD_SESSMGR_RLSSS                                  0x20000000000290L
#define PMD_SESSMGR_RLSSS_I                                0x20000000000291L
#define PMD_SESSMGR_REPLY                                  0x20000000000292L
#define PMD_SESSMGR_HDLSNCLOSE                             0x20000000000293L
#define PMD_SESSMGR_HDLSTOP                                0x20000000000294L
#define PMD_SESSMGR_HDLSNTM                                0x20000000000295L
#define PMD_SESSMGR_CHKSNMETA                              0x20000000000296L
#define PMD_SESSMGR_CHKFORCESN                             0x20000000000297L
#define PMD_SESSMGR_CHKSN                                  0x20000000000298L
#define SDB_PMDWINSTARTSVC                                 0x20000000000299L
#define SDB_PMDWINSVC_STPSVC                               0x2000000000029aL
#define SDB_PMDWINSVCMAIN                                  0x2000000000029bL
#define SDB_PMDWINSVCREPSTATTOMGR                          0x2000000000029cL
#define SDB_PMDWINSVCCTRLHANDL                             0x2000000000029dL
#define SDB_SDBSTOP_RESVARG                                0x2000000000029eL
#define SDB_SDBSTOP_MAIN                                   0x2000000000029fL
#define SDB_PMDASYNCNETEP                                  0x200000000002a0L
#define SDB__PMDMEMPOL_ALLOC                               0x200000000002a1L
#define SDB__PMDMEMPOL_RELEASE                             0x200000000002a2L
#define SDB__PMDMEMPOL_REALLOC                             0x200000000002a3L
#define SDB_PMDRESVARGS                                    0x200000000002a4L
#define SDB_PMDMSTTHRDMAIN                                 0x200000000002a5L
#define SDB_PMDMAIN                                        0x200000000002a6L
#define SDB_CMSTOP_TERMPROC                                0x200000000002a7L
#define SDB_CMSTOP_MAIN                                    0x200000000002a8L
#define SDB_CMMINTHREADENTY                                0x200000000002a9L
#define SDB_PMDPRELOADERENENTPNT                           0x200000000002aaL
#endif
