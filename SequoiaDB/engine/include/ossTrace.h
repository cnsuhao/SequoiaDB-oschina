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
#ifndef ossTRACE_H__
#define ossTRACE_H__
#define SDB__OSSMEMALLOC                                   0x1000000054eL
#define SDB__OSSMEMREALLOC                                 0x1000000054fL
#define SDB__OSSMEMFREE                                    0x10000000550L
#define SDB_OSSMODULEHANDLE_INIT                           0x10000000551L
#define SDB_OSSMODULEHANDLE_UNLOAD                         0x10000000552L
#define SDB_OSSMODULEHANDLE_RESOLVEADDRESS                 0x10000000553L
#define SDB__OSSMMF_OPEN                                   0x10000000554L
#define SDB__OSSMMF_CLOSE                                  0x10000000555L
#define SDB__OSSMMF_SIZE                                   0x10000000556L
#define SDB__OSSMMF_MAP                                    0x10000000557L
#define SDB__OSSMMF_FLHALL                                 0x10000000558L
#define SDB__OSSMMF_FLUSH                                  0x10000000559L
#define SDB__OSSMMF_UNLINK                                 0x1000000055aL
#define SDB_OSSISPROCRUNNING                               0x1000000055bL
#define SDB_OSSWAITCHLD                                    0x1000000055cL
#define SDB_OSSCRTLST                                      0x1000000055dL
#define SDB_OSSEXEC2                                       0x1000000055eL
#define SDB_OSSEXEC                                        0x1000000055fL
#define SDB_OSSENBNMCHGS                                   0x10000000560L
#define SDB_OSSRENMPROC                                    0x10000000561L
#define SDB_OSSVERIFYPID                                   0x10000000562L
#define SDB_OSSRSVPATH                                     0x10000000563L
#define SDB_OSSWTINT                                       0x10000000564L
#define SDB_OSSSTARTSERVICE                                0x10000000565L
#define SDB_OSS_WFSTRS                                     0x10000000566L
#define SDB_OSS_STOPSERVICE                                0x10000000567L
#define SDB_OSSCRTPADUPHND                                 0x10000000568L
#define SDB_WIN_OSSEXEC                                    0x10000000569L
#define SDB_OSSGETEWD                                      0x1000000056aL
#define SDB_OSSCMSTART_BLDARGS                             0x1000000056bL
#define SDB_OSS_STARTPROCESS                               0x1000000056cL
#define SDB_OSSNTHND                                       0x1000000056dL
#define SDB_OSSST                                          0x1000000056eL
#define SDB_OSSEDUCTHND                                    0x1000000056fL
#define SDB_OSSEDUEXCFLT                                   0x10000000570L
#define SDB_OSSDMPSYSTM                                    0x10000000571L
#define SDB_OSSDMPDBINFO                                   0x10000000572L
#define SDB_OSSSTKTRA                                      0x10000000573L
#define SDB_OSSREGSIGHND                                   0x10000000574L
#define SDB_OSSRSTSYSSIG                                   0x10000000575L
#define SDB_OSSSIGHNDABT                                   0x10000000576L
#define SDB_OSSFUNCADDR2NM                                 0x10000000577L
#define SDB_OSSDUMPSYSTM                                   0x10000000578L
#define SDB_OSSDUMPDBINFO                                  0x10000000579L
#define SDB_OSSDUMPSYSINFO                                 0x1000000057aL
#define SDB_OSSMCHCODE                                     0x1000000057bL
#define SDB_OSSDUMPSIGINFO                                 0x1000000057cL
#define SDB_OSSWLKSTK                                      0x1000000057dL
#define SDB_OSSGETSYMBNFA                                  0x1000000057eL
#define SDB_OSSDUMPREGSINFO                                0x1000000057fL
#define SDB_OSSDUMPST                                      0x10000000580L
#define SDB_OSSDUMPREGSINFO2                               0x10000000581L
#define SDB_OSSDUMPST2                                     0x10000000582L
#define SDB_OSSDUMPREGSINFO3                               0x10000000583L
#define SDB_OSSDUMPST3                                     0x10000000584L
#define SDB_OSSSYMINIT                                     0x10000000585L
#define SDB_OSSWKSEX                                       0x10000000586L
#define SDB_OSSWS                                          0x10000000587L
#define SDB_OSSGETSYMBNFADDR                               0x10000000588L
#define SDB_GETEXECNM                                      0x10000000589L
#define SDB_OSSLCEXEC                                      0x1000000058aL
#define SDB_OSSOPEN                                        0x1000000058bL
#define SDB_OSSCLOSE                                       0x1000000058cL
#define SDB_OSSMKDIR                                       0x1000000058dL
#define SDB_OSSDELETE                                      0x1000000058eL
#define SDB_OSSFILECOPY                                    0x1000000058fL
#define SDB_OSSACCESS                                      0x10000000590L
#define SDB_OSSREAD                                        0x10000000591L
#define SDB_OSSWRITE                                       0x10000000592L
#define SDB_OSSSEEK                                        0x10000000593L
#define SDB_OSSSEEKANDREAD                                 0x10000000594L
#define SDB_OSSSEEKANDWRITE                                0x10000000595L
#define SDB_OSSFSYNC                                       0x10000000596L
#define SDB_OSSGETPATHTYPE                                 0x10000000597L
#define SDB_OSSGETFSBYNM                                   0x10000000598L
#define SDB_OSSGETFILESIZE                                 0x10000000599L
#define SDB_OSSTRUNCATEFILE                                0x1000000059aL
#define SDB_OSSEXTFILE                                     0x1000000059bL
#define SDB_OSSGETREALPATH                                 0x1000000059cL
#define SDB_OSSGETFSTYPE                                   0x1000000059dL
#define SDB_OSSRENMPATH                                    0x1000000059eL
#define SDB_OSSLOCKFILE                                    0x1000000059fL
#define SDB__OSSENUMNMPS                                   0x100000005a0L
#define SDB__OSSENUMNMPS2                                  0x100000005a1L
#define SDB_OSSCRTNMP                                      0x100000005a2L
#define SDB_OSSOPENNMP                                     0x100000005a3L
#define SDB_OSSCONNNMP                                     0x100000005a4L
#define SDB_OSSRENMP                                       0x100000005a5L
#define SDB_OSSWTNMP                                       0x100000005a6L
#define SDB_OSSDISCONNNMP                                  0x100000005a7L
#define SDB_OSSCLSNMP                                      0x100000005a8L
#define SDB_OSSNMP2FD                                      0x100000005a9L
#define SDB_OSSCRTNP                                       0x100000005aaL
#define SDB_OSSOPENNP                                      0x100000005abL
#define SDB_OSSRDNP                                        0x100000005acL
#define SDB__OSSWTNP                                       0x100000005adL
#define SDB_OSSDELNP                                       0x100000005aeL
#define SDB_OSSNP2FD                                       0x100000005afL
#define SDB_OSSCLNPBYNM                                    0x100000005b0L
#define SDB__OSSSK__OSSSK                                  0x100000005b1L
#define SDB__OSSSK__OSSSK2                                 0x100000005b2L
#define SDB__OSSSK__OSSSK3                                 0x100000005b3L
#define SDB_OSSSK_INITTSK                                  0x100000005b4L
#define SDB_OSSSK_SETSKLI                                  0x100000005b5L
#define SDB_OSSSK_BIND_LSTN                                0x100000005b6L
#define SDB_OSSSK_SEND                                     0x100000005b7L
#define SDB_OSSSK_ISCONN                                   0x100000005b8L
#define SDB_OSSSK_CONNECT                                  0x100000005b9L
#define SDB_OSSSK_CLOSE                                    0x100000005baL
#define SDB_OSSSK_DISNAG                                   0x100000005bbL
#define SDB_OSSSK__GETADDR                                 0x100000005bcL
#define SDB_OSSSK_SETTMOUT                                 0x100000005bdL
#define SDB__OSSSK__COMPLETE                               0x100000005beL
#define SDB_OSSPFOP_OPEN                                   0x100000005bfL
#define SDB_OSSPFOP_READ                                   0x100000005c0L
#define SDB_OSSPFOP_WRITE                                  0x100000005c1L
#define SDB_OSSPFOP_FWRITE                                 0x100000005c2L
#define SDB_OSSPFOP_GETSIZE                                0x100000005c3L
#define SDB__OSSRWM_LOCK_R                                 0x100000005c4L
#define SDB__OSSRWM_LOCK_W                                 0x100000005c5L
#define SDB__OSSRWM_RLS_R                                  0x100000005c6L
#define SDB__OSSRWM_RLS_W                                  0x100000005c7L
#define SDB__OSSAIOMSGPROC__PROC                           0x100000005c8L
#define SDB__OSSAIOMSGPROC__HNDWP                          0x100000005c9L
#define SDB__OSSAIOMSGPROC__HNDRPH                         0x100000005caL
#define SDB__OSSAIOMSGPROC__RDPH                           0x100000005cbL
#define SDB__OSSAIOMSGPROC_CONNECT                         0x100000005ccL
#define SDB__TMPAIR_CHK_DLINE                              0x100000005cdL
#define SDB__TMPAIR_RUN                                    0x100000005ceL
#define SDB__OSSAIO__HNDAPT                                0x100000005cfL
#define SDB__OSSAIO__ACCEPT                                0x100000005d0L
#define SDB__OSSAIO_CONNECT                                0x100000005d1L
#define SDB__OSSAIO_ADDTIMER                               0x100000005d2L
#define SDB__OSSAIO_RMTIMER                                0x100000005d3L
#define SDB_OSSTS2STR                                      0x100000005d4L
#define SDB_OSSGETCPUUSG                                   0x100000005d5L
#define SDB_OSSGETCPUUSG2                                  0x100000005d6L
#define SDB_OSSTCF_INIT                                    0x100000005d7L
#define SDB_OSSSRAND                                       0x100000005d8L
#define SDB_OSSRAND                                        0x100000005d9L
#define SDB_OSSHEXDL                                       0x100000005daL
#define SDB_OSSHEXDUMPBUF                                  0x100000005dbL
#define SDB_OSSGETMEMINFO                                  0x100000005dcL
#define SDB_OSSGETDISKINFO                                 0x100000005ddL
#define SDB_OSSGETCPUINFO                                  0x100000005deL
#define SDB_OSSGETPROCMEMINFO                              0x100000005dfL
#define SDB__OSSEVN_WAIT                                   0x100000005e0L
#define SDB__OSSEN_SIGNAL                                  0x100000005e1L
#define SDB__OSSEN_SIGALL                                  0x100000005e2L
#define SDB__OSSVN_RESET                                   0x100000005e3L
#endif
