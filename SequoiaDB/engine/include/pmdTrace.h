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
#ifndef pmdTRACE_H__
#define pmdTRACE_H__
#define SDB_PMDLOGGWENTPNT                                 0x20000000000420L
#define SDB_PMDPIPELSTNNPNTPNT                             0x20000000000421L
#define SDB_READFILE                                       0x20000000000422L
#define SDB_PARSEARGUMENTS                                 0x20000000000423L
#define SDB_ENTERBATCHMODE                                 0x20000000000424L
#define SDB_ENTERINTATVMODE                                0x20000000000425L
#define SDB_FORMATARGS                                     0x20000000000426L
#define SDB_CREATEDAEMONPROC                               0x20000000000427L
#define SDB_ENTERFRONTENDMODE                              0x20000000000428L
#define SDB_SDB_MAIN                                       0x20000000000429L
#define SDB_PMDPRELOADERENENTPNT                           0x2000000000042aL
#define SDB_PMDBGJOBENTPNT                                 0x2000000000042bL
#define SDB_REGEDUNAME                                     0x2000000000042cL
#define SDB__PMDEDUCB_DISCONNECT                           0x2000000000042dL
#define SDB__PMDEDUCB_FORCE                                0x2000000000042eL
#define SDB__PMDEDUCB_ISINT                                0x2000000000042fL
#define SDB__PMDEDUCB_PRINTINFO                            0x20000000000430L
#define SDB__PMDEDUCB_GETINFO                              0x20000000000431L
#define SDB__PMDEDUCB_RESETINFO                            0x20000000000432L
#define SDB__PMDEDUCB_CONTXTPEEK                           0x20000000000433L
#define SDB___PMDEDUCB_DUMPINFO                            0x20000000000434L
#define SDB___PMDEDUCB_DUMPINFO2                           0x20000000000435L
#define SDB__PMDEDUCB_GETTRANSLOCK                         0x20000000000436L
#define SDB__PMDEDUCB_ADDLOCKINFO                          0x20000000000437L
#define SDB__PMDEDUCB_DELLOCKINFO                          0x20000000000438L
#define SDB__PMDEDUCB_CREATETRANSACTION                    0x20000000000439L
#define SDB__PMDEDUCB_DELTRANSACTION                       0x2000000000043aL
#define SDB__PMDEDUCB_ADDTRANSNODE                         0x2000000000043bL
#define SDB__PMDEDUCB_GETTRANSNODEROUTEID                  0x2000000000043cL
#define SDB__PMDEDUCB_ISTRANSNODE                          0x2000000000043dL
#define SDB_PMDEDUENTPNT                                   0x2000000000043eL
#define SDB_PMDRECV                                        0x2000000000043fL
#define SDB_PMDSEND                                        0x20000000000440L
#define SDB_PMDCOORDNETWKENTPNT                            0x20000000000441L
#define SDB_PMDWINSTARTSVC                                 0x20000000000442L
#define SDB_PMDWINSVC_STPSVC                               0x20000000000443L
#define SDB_PMDWINSVCMAIN                                  0x20000000000444L
#define SDB_PMDWINSVCREPSTATTOMGR                          0x20000000000445L
#define SDB_PMDWINSVCCTRLHANDL                             0x20000000000446L
#define SDB_SDBSTART_RESVARG                               0x20000000000447L
#define SDB_SDBSTART_MAIN                                  0x20000000000448L
#define SDB__PMDMEMPOL_ALLOC                               0x20000000000449L
#define SDB__PMDMEMPOL_RELEASE                             0x2000000000044aL
#define SDB__PMDMEMPOL_REALLOC                             0x2000000000044bL
#define SDB_SDBLOAD_RESOLVEARG                             0x2000000000044cL
#define SDB_SDBLOAD_LOADRECV                               0x2000000000044dL
#define SDB_SDBLOAD_CONNECTSDB                             0x2000000000044eL
#define SDB_SDBLOAD_MAIN                                   0x2000000000044fL
#define SDB_PMDRESVARGS                                    0x20000000000450L
#define SDB_PMDMSTTHRDMAIN                                 0x20000000000451L
#define SDB_PMDMAIN                                        0x20000000000452L
#define SDB_PMDTCPLSTNENTPNT                               0x20000000000453L
#define SDB__PMDOPTMGR_INIT                                0x20000000000454L
#define SDB__PMDOPTMGR__MKDIR                              0x20000000000455L
#define SDB__PMDOPTMGR_REFLUSH2FILE                        0x20000000000456L
#define SDB_PMDCBMGREP                                     0x20000000000457L
#define SDB_SDBLIST_RESVARG                                0x20000000000458L
#define SDB_SDBLIST_MAIN                                   0x20000000000459L
#define SDB_SDBINSPT_RESVARG                               0x2000000000045aL
#define SDB_FLUSHOUTPUT                                    0x2000000000045bL
#define SDB_DUMPPRINTF                                     0x2000000000045cL
#define SDB_REALLOCBUFFER                                  0x2000000000045dL
#define SDB_GETEXTBUFFER                                   0x2000000000045eL
#define SDB_INSPECTHEADER                                  0x2000000000045fL
#define SDB_DUMPHEADER                                     0x20000000000460L
#define SDB_INSPECTSME                                     0x20000000000461L
#define SDB_DUMPSME                                        0x20000000000462L
#define SDB_GETEXTENTHEAD                                  0x20000000000463L
#define SDB_GETEXTENT                                      0x20000000000464L
#define SDB_EXTENTSANITYCHK                                0x20000000000465L
#define SDB_LOADMB                                         0x20000000000466L
#define SDB_LOADEXTENT                                     0x20000000000467L
#define SDB_INSPOVFLWRECRDS                                0x20000000000468L
#define SDB_DUMPOVFWRECRDS                                 0x20000000000469L
#define SDB_INSPINXDEF                                     0x2000000000046aL
#define SDB_DUMPINXDEF                                     0x2000000000046bL
#define SDB_INSPINXEXTS                                    0x2000000000046cL
#define SDB_DUMPINXEXTS                                    0x2000000000046dL
#define SDB_INSPCOLL                                       0x2000000000046eL
#define SDB_DUMPCOLL                                       0x2000000000046fL
#define SDB_INSPCOLLS                                      0x20000000000470L
#define SDB_DUMPCOLLS                                      0x20000000000471L
#define SDB_INSPCOLLECTIONS                                0x20000000000472L
#define SDB_DUMPRAWPAGE                                    0x20000000000473L
#define SDB_DUMPCOLLECTIONS                                0x20000000000474L
#define SDB_ACTIONCSATTEMPT                                0x20000000000475L
#define SDB_DUMPPAGES                                      0x20000000000476L
#define SDB_DUMPDB                                         0x20000000000477L
#define SDB_INSPECTDB                                      0x20000000000478L
#define SDB_SDBINSPT_MAIN                                  0x20000000000479L
#define SDB_PMDSIGHND                                      0x2000000000047aL
#define SDB_PMDEDUUSERTRAPHNDL                             0x2000000000047bL
#define SDB_PMDCTRLHND                                     0x2000000000047cL
#define SDB_PMDSYNCSESSIONAGENTEP                          0x2000000000047dL
#define SDB_SDBSTOP_RESVARG                                0x2000000000047eL
#define SDB_SDBSTOP_MAIN                                   0x2000000000047fL
#define SDB_READFROMPIPE                                   0x20000000000480L
#define SDB_MONITOR_THREAD                                 0x20000000000481L
#define SDB_CREATESHMONTHREAD                              0x20000000000482L
#define SDB_ENTERDAEMONMODE                                0x20000000000483L
#define SDB_SDBBP_MAIN                                     0x20000000000484L
#define SDB__PMDSTARTUP_INIT                               0x20000000000485L
#define SDB__PMDEDUMGR_DELIOSVC                            0x20000000000486L
#define SDB__PMDEDUMGR_DUMPINFO                            0x20000000000487L
#define SDB__PMDEDUMGR_DUMPINFO2                           0x20000000000488L
#define SDB__PMDEDUMGR_DESTROYALL                          0x20000000000489L
#define SDB__PMDEDUMGR_FORCEUSREDU                         0x2000000000048aL
#define SDB__PMDEDUMGR__FORCEIOSVC                         0x2000000000048bL
#define SDB__PMDEDUMGR__FORCEEDUS                          0x2000000000048cL
#define SDB__PMDEDUMGR__GETEDUCNT                          0x2000000000048dL
#define SDB__PMDEDUMGR_PSTEDUPST                           0x2000000000048eL
#define SDB__PMDEDUMGR_WAITEDUPST                          0x2000000000048fL
#define SDB__PMDEDUMGR_RTNEDU                              0x20000000000490L
#define SDB__PMDEDUMGR_STARTEDU                            0x20000000000491L
#define SDB__PMDEDUMGR_CRTNEWEDU                           0x20000000000492L
#define SDB__PMDEDUMGR_DSTEDU                              0x20000000000493L
#define SDB__PMDEDUMGR_WAITEDU                             0x20000000000494L
#define SDB__PMDEDUMGR_WAITEDU2                            0x20000000000495L
#define SDB__PMDEDUMGR_DEATVEDU                            0x20000000000496L
#define SDB__PMDEDUMGR_ATVEDU                              0x20000000000497L
#define SDB__PMDEDUMGR_ATVEDU2                             0x20000000000498L
#define SDB__PMDEDUMGR_WAITUTIL                            0x20000000000499L
#define SDB__PMDEDUMGR_WAITUTIL2                           0x2000000000049aL
#define SDB__PMDEDUMGR_GETEDUTRDID                         0x2000000000049bL
#define SDB_PMDASYNCNETEP                                  0x2000000000049cL
#define SDB_PMDLOADWORKER                                  0x2000000000049dL
#define SDB__PMDTMHD                                       0x2000000000049eL
#define SDB__PMDTMHD_DES                                   0x2000000000049fL
#define SDB__PMDTMHD_HDTMOUT                               0x200000000004a0L
#define SDB__PMDMSGHND                                     0x200000000004a1L
#define SDB__PMDMSGHND_DESC                                0x200000000004a2L
#define SDB__PMDMSGHND_CPMSG                               0x200000000004a3L
#define SDB__PMDMSGHND_HNDMSG                              0x200000000004a4L
#define SDB__PMDMSGHND_HNDCLOSE                            0x200000000004a5L
#define SDB__PMDMSGHND_ONSTOP                              0x200000000004a6L
#define SDB__PMDMSGHND_HNDSNMSG                            0x200000000004a7L
#define SDB__PMDMSGHND_HNDMAINMSG                          0x200000000004a8L
#define SDB__PMDMSGHND_POSTMAINMSG                         0x200000000004a9L
#define SDB__PMDSN                                         0x200000000004aaL
#define SDB__PMDSN_DESC                                    0x200000000004abL
#define SDB__PMDSN_ATHIN                                   0x200000000004acL
#define SDB__PMDSN_ATHOUT                                  0x200000000004adL
#define SDB__PMDSN_CLEAR                                   0x200000000004aeL
#define SDB__PMDSN__MKNAME                                 0x200000000004afL
#define SDB__PMDSN__LOCK                                   0x200000000004b0L
#define SDB__PMDSN__UNLOCK                                 0x200000000004b1L
#define SDB__PMDSN_WTATH                                   0x200000000004b2L
#define SDB__PMDSN_WTDTH                                   0x200000000004b3L
#define SDB__PMDSN_CPMSG                                   0x200000000004b4L
#define SDB__PMDSN_FRNBUF                                  0x200000000004b5L
#define SDB__PMDSN_POPBUF                                  0x200000000004b6L
#define SDB__PMDSN_PSHBUF                                  0x200000000004b7L
#define PMD_SESSMGR                                        0x200000000004b8L
#define PMD_SESSMGR_DESC                                   0x200000000004b9L
#define PMD_SESSMGR_INIT                                   0x200000000004baL
#define PMD_SESSMGR_FINI                                   0x200000000004bbL
#define PMD_SESSMGR_FORCENTY                               0x200000000004bcL
#define PMD_SESSMGR_ONTIMER                                0x200000000004bdL
#define PMD_SESSMGR_PUSHMSG                                0x200000000004beL
#define PMD_SESSMGR_GETSESSION                             0x200000000004bfL
#define CLS_PMDSMGR_ATCHMETA                               0x200000000004c0L
#define PMD_SESSMGR_STARTEDU                               0x200000000004c1L
#define PMD_SESSMGR_RLSSS                                  0x200000000004c2L
#define PMD_SESSMGR_RLSSS_I                                0x200000000004c3L
#define PMD_SESSMGR_REPLY                                  0x200000000004c4L
#define PMD_SESSMGR_HDLSNCLOSE                             0x200000000004c5L
#define PMD_SESSMGR_HDLSTOP                                0x200000000004c6L
#define PMD_SESSMGR_HDLSNTM                                0x200000000004c7L
#define PMD_SESSMGR_CHKSNMETA                              0x200000000004c8L
#define PMD_SESSMGR_CHKFORCESN                             0x200000000004c9L
#define PMD_SESSMGR_CHKSN                                  0x200000000004caL
#define SDB_CMSTOP_TERMPROC                                0x200000000004cbL
#define SDB_CMSTOP_MAIN                                    0x200000000004ccL
#define SDB_CMMINTHREADENTY                                0x200000000004cdL
#define SDB_PMDHANDLESYSINFOREQUEST                        0x200000000004ceL
#define SDB_PMDPROCCOORDAGENTREQ                           0x200000000004cfL
#define SDB_PMDPROCAGENTREQ                                0x200000000004d0L
#define SDB_PMDHTTPAGENTENTPNT                             0x200000000004d1L
#define SDB_PMDAUTHENTICATE                                0x200000000004d2L
#define SDB_PMDAGENTENTPNT                                 0x200000000004d3L
#define SDB_PMDLOCALAGENTENTPNT                            0x200000000004d4L
#endif
