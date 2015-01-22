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
#define SDB_READFILE                                       0x2000000000004eL
#define SDB_PARSEARGUMENTS                                 0x2000000000004fL
#define SDB_ENTERBATCHMODE                                 0x20000000000050L
#define SDB_ENTERINTATVMODE                                0x20000000000051L
#define SDB_FORMATARGS                                     0x20000000000052L
#define SDB_CREATEDAEMONPROC                               0x20000000000053L
#define SDB_ENTERFRONTENDMODE                              0x20000000000054L
#define SDB_SDB_MAIN                                       0x20000000000055L
#define SDB__PMDSN                                         0x20000000000056L
#define SDB__PMDSN_DESC                                    0x20000000000057L
#define SDB__PMDSN_ATHIN                                   0x20000000000058L
#define SDB__PMDSN_ATHOUT                                  0x20000000000059L
#define SDB__PMDSN_CLEAR                                   0x2000000000005aL
#define SDB__PMDSN__MKNAME                                 0x2000000000005bL
#define SDB__PMDSN__LOCK                                   0x2000000000005cL
#define SDB__PMDSN__UNLOCK                                 0x2000000000005dL
#define SDB__PMDSN_WTATH                                   0x2000000000005eL
#define SDB__PMDSN_WTDTH                                   0x2000000000005fL
#define SDB__PMDSN_CPMSG                                   0x20000000000060L
#define SDB__PMDSN_FRNBUF                                  0x20000000000061L
#define SDB__PMDSN_POPBUF                                  0x20000000000062L
#define SDB__PMDSN_PSHBUF                                  0x20000000000063L
#define PMD_SESSMGR                                        0x20000000000064L
#define PMD_SESSMGR_DESC                                   0x20000000000065L
#define PMD_SESSMGR_INIT                                   0x20000000000066L
#define PMD_SESSMGR_FINI                                   0x20000000000067L
#define PMD_SESSMGR_FORCENTY                               0x20000000000068L
#define PMD_SESSMGR_ONTIMER                                0x20000000000069L
#define PMD_SESSMGR_PUSHMSG                                0x2000000000006aL
#define PMD_SESSMGR_GETSESSION                             0x2000000000006bL
#define CLS_PMDSMGR_ATCHMETA                               0x2000000000006cL
#define PMD_SESSMGR_STARTEDU                               0x2000000000006dL
#define PMD_SESSMGR_RLSSS                                  0x2000000000006eL
#define PMD_SESSMGR_RLSSS_I                                0x2000000000006fL
#define PMD_SESSMGR_REPLY                                  0x20000000000070L
#define PMD_SESSMGR_HDLSNCLOSE                             0x20000000000071L
#define PMD_SESSMGR_HDLSTOP                                0x20000000000072L
#define PMD_SESSMGR_HDLSNTM                                0x20000000000073L
#define PMD_SESSMGR_CHKSNMETA                              0x20000000000074L
#define PMD_SESSMGR_CHKFORCESN                             0x20000000000075L
#define PMD_SESSMGR_CHKSN                                  0x20000000000076L
#define SDB_SDBINSPT_RESVARG                               0x20000000000077L
#define SDB_FLUSHOUTPUT                                    0x20000000000078L
#define SDB_DUMPPRINTF                                     0x20000000000079L
#define SDB_REALLOCBUFFER                                  0x2000000000007aL
#define SDB_GETEXTBUFFER                                   0x2000000000007bL
#define SDB_INSPECTHEADER                                  0x2000000000007cL
#define SDB_DUMPHEADER                                     0x2000000000007dL
#define SDB_INSPECTSME                                     0x2000000000007eL
#define SDB_DUMPSME                                        0x2000000000007fL
#define SDB_GETEXTENTHEAD                                  0x20000000000080L
#define SDB_GETEXTENT                                      0x20000000000081L
#define SDB_EXTENTSANITYCHK                                0x20000000000082L
#define SDB_LOADMB                                         0x20000000000083L
#define SDB_LOADEXTENT                                     0x20000000000084L
#define SDB_INSPOVFLWRECRDS                                0x20000000000085L
#define SDB_DUMPOVFWRECRDS                                 0x20000000000086L
#define SDB_INSPINXDEF                                     0x20000000000087L
#define SDB_DUMPINXDEF                                     0x20000000000088L
#define SDB_INSPINXEXTS                                    0x20000000000089L
#define SDB_DUMPINXEXTS                                    0x2000000000008aL
#define SDB_INSPCOLL                                       0x2000000000008bL
#define SDB_DUMPCOLL                                       0x2000000000008cL
#define SDB_INSPCOLLS                                      0x2000000000008dL
#define SDB_DUMPCOLLS                                      0x2000000000008eL
#define SDB_INSPCOLLECTIONS                                0x2000000000008fL
#define SDB_DUMPRAWPAGE                                    0x20000000000090L
#define SDB_DUMPCOLLECTIONS                                0x20000000000091L
#define SDB_ACTIONCSATTEMPT                                0x20000000000092L
#define SDB_DUMPPAGES                                      0x20000000000093L
#define SDB_DUMPDB                                         0x20000000000094L
#define SDB_INSPECTDB                                      0x20000000000095L
#define SDB_SDBINSPT_MAIN                                  0x20000000000096L
#define SDB_PMDSIGHND                                      0x20000000000097L
#define SDB_PMDEDUUSERTRAPHNDL                             0x20000000000098L
#define SDB_PMDCTRLHND                                     0x20000000000099L
#define SDB__PMDOPTMGR_INIT                                0x2000000000009aL
#define SDB__PMDOPTMGR__MKDIR                              0x2000000000009bL
#define SDB__PMDOPTMGR_REFLUSH2FILE                        0x2000000000009cL
#define SDB__PMDSTARTUP_INIT                               0x2000000000009dL
#define SDB__PMDSTARTUP_FINAL                              0x2000000000009eL
#define SDB_PMDWINSTARTSVC                                 0x2000000000009fL
#define SDB_PMDWINSVC_STPSVC                               0x200000000000a0L
#define SDB_PMDWINSVCMAIN                                  0x200000000000a1L
#define SDB_PMDWINSVCREPSTATTOMGR                          0x200000000000a2L
#define SDB_PMDWINSVCCTRLHANDL                             0x200000000000a3L
#define SDB_SDBLOAD_RESOLVEARG                             0x200000000000a4L
#define SDB_SDBLOAD_LOADRECV                               0x200000000000a5L
#define SDB_SDBLOAD_CONNECTSDB                             0x200000000000a6L
#define SDB_SDBLOAD_MAIN                                   0x200000000000a7L
#define SDB_SDBSTART_RESVARG                               0x200000000000a8L
#define SDB_SDBSTART_MAIN                                  0x200000000000a9L
#define SDB_PMDLOCALAGENTENTPNT                            0x200000000000aaL
#define SDB__PMDMEMPOL_ALLOC                               0x200000000000abL
#define SDB__PMDMEMPOL_RELEASE                             0x200000000000acL
#define SDB__PMDMEMPOL_REALLOC                             0x200000000000adL
#define SDB_PMDTCPLSTNENTPNT                               0x200000000000aeL
#define SDB__PMDEDUMGR_DELIOSVC                            0x200000000000afL
#define SDB__PMDEDUMGR_DUMPINFO                            0x200000000000b0L
#define SDB__PMDEDUMGR_DUMPINFO2                           0x200000000000b1L
#define SDB__PMDEDUMGR_DESTROYALL                          0x200000000000b2L
#define SDB__PMDEDUMGR_FORCEUSREDU                         0x200000000000b3L
#define SDB__PMDEDUMGR__FORCEIOSVC                         0x200000000000b4L
#define SDB__PMDEDUMGR__FORCEEDUS                          0x200000000000b5L
#define SDB__PMDEDUMGR__GETEDUCNT                          0x200000000000b6L
#define SDB__PMDEDUMGR_PSTEDUPST                           0x200000000000b7L
#define SDB__PMDEDUMGR_WAITEDUPST                          0x200000000000b8L
#define SDB__PMDEDUMGR_RTNEDU                              0x200000000000b9L
#define SDB__PMDEDUMGR_STARTEDU                            0x200000000000baL
#define SDB__PMDEDUMGR_CRTNEWEDU                           0x200000000000bbL
#define SDB__PMDEDUMGR_DSTEDU                              0x200000000000bcL
#define SDB__PMDEDUMGR_WAITEDU                             0x200000000000bdL
#define SDB__PMDEDUMGR_WAITEDU2                            0x200000000000beL
#define SDB__PMDEDUMGR_DEATVEDU                            0x200000000000bfL
#define SDB__PMDEDUMGR_ATVEDU                              0x200000000000c0L
#define SDB__PMDEDUMGR_ATVEDU2                             0x200000000000c1L
#define SDB__PMDEDUMGR_WAITUTIL                            0x200000000000c2L
#define SDB__PMDEDUMGR_WAITUTIL2                           0x200000000000c3L
#define SDB__PMDEDUMGR_GETEDUTRDID                         0x200000000000c4L
#define SDB_PMDRESVARGS                                    0x200000000000c5L
#define SDB_PMDMSTTHRDMAIN                                 0x200000000000c6L
#define SDB_PMDMAIN                                        0x200000000000c7L
#define SDB_SDBLIST_RESVARG                                0x200000000000c8L
#define SDB_SDBLIST_MAIN                                   0x200000000000c9L
#define SDB_PMDASYNCNETEP                                  0x200000000000caL
#define SDB__PMDTMHD                                       0x200000000000cbL
#define SDB__PMDTMHD_DES                                   0x200000000000ccL
#define SDB__PMDTMHD_HDTMOUT                               0x200000000000cdL
#define SDB__PMDMSGHND                                     0x200000000000ceL
#define SDB__PMDMSGHND_DESC                                0x200000000000cfL
#define SDB__PMDMSGHND_CPMSG                               0x200000000000d0L
#define SDB__PMDMSGHND_HNDMSG                              0x200000000000d1L
#define SDB__PMDMSGHND_HNDCLOSE                            0x200000000000d2L
#define SDB__PMDMSGHND_ONSTOP                              0x200000000000d3L
#define SDB__PMDMSGHND_HNDSNMSG                            0x200000000000d4L
#define SDB__PMDMSGHND_HNDMAINMSG                          0x200000000000d5L
#define SDB__PMDMSGHND_POSTMAINMSG                         0x200000000000d6L
#define SDB_REGEDUNAME                                     0x200000000000d7L
#define SDB__PMDEDUCB_DISCONNECT                           0x200000000000d8L
#define SDB__PMDEDUCB_FORCE                                0x200000000000d9L
#define SDB__PMDEDUCB_ISINT                                0x200000000000daL
#define SDB__PMDEDUCB_PRINTINFO                            0x200000000000dbL
#define SDB__PMDEDUCB_GETINFO                              0x200000000000dcL
#define SDB__PMDEDUCB_RESETINFO                            0x200000000000ddL
#define SDB__PMDEDUCB_CONTXTPEEK                           0x200000000000deL
#define SDB___PMDEDUCB_DUMPINFO                            0x200000000000dfL
#define SDB___PMDEDUCB_DUMPINFO2                           0x200000000000e0L
#define SDB__PMDEDUCB_GETTRANSLOCK                         0x200000000000e1L
#define SDB__PMDEDUCB_ADDLOCKINFO                          0x200000000000e2L
#define SDB__PMDEDUCB_DELLOCKINFO                          0x200000000000e3L
#define SDB__PMDEDUCB_CREATETRANSACTION                    0x200000000000e4L
#define SDB__PMDEDUCB_DELTRANSACTION                       0x200000000000e5L
#define SDB__PMDEDUCB_ADDTRANSNODE                         0x200000000000e6L
#define SDB__PMDEDUCB_GETTRANSNODEROUTEID                  0x200000000000e7L
#define SDB__PMDEDUCB_ISTRANSNODE                          0x200000000000e8L
#define SDB_PMDEDUENTPNT                                   0x200000000000e9L
#define SDB_PMDRECV                                        0x200000000000eaL
#define SDB_PMDSEND                                        0x200000000000ebL
#define SDB_SDBSTOP_RESVARG                                0x200000000000ecL
#define SDB_SDBSTOP_MAIN                                   0x200000000000edL
#define SDB_CMMINTHREADENTY                                0x200000000000eeL
#define SDB_CMSTOP_TERMPROC                                0x200000000000efL
#define SDB_CMSTOP_MAIN                                    0x200000000000f0L
#define SDB_PMDPIPELSTNNPNTPNT                             0x200000000000f1L
#define SDB_PMDBGJOBENTPNT                                 0x200000000000f2L
#define SDB_PMDCBMGREP                                     0x200000000000f3L
#define SDB_PMDCOORDNETWKENTPNT                            0x200000000000f4L
#define SDB_PMDPRELOADERENENTPNT                           0x200000000000f5L
#define SDB_PMDSYNCSESSIONAGENTEP                          0x200000000000f6L
#define SDB_PMDLOGGWENTPNT                                 0x200000000000f7L
#define SDB_PMDLOADWORKER                                  0x200000000000f8L
#define SDB_READFROMPIPE                                   0x200000000000f9L
#define SDB_MONITOR_THREAD                                 0x200000000000faL
#define SDB_CREATESHMONTHREAD                              0x200000000000fbL
#define SDB_ENTERDAEMONMODE                                0x200000000000fcL
#define SDB_SDBBP_MAIN                                     0x200000000000fdL
#endif
