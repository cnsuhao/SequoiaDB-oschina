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
#define SDB_OSSRSTSYSSIG                                   0x1000000080aL
#define SDB_OSSSIGHNDABT                                   0x1000000080bL
#define SDB_OSSFUNCADDR2NM                                 0x1000000080cL
#define SDB_OSSDUMPSYSTM                                   0x1000000080dL
#define SDB_OSSDUMPDBINFO                                  0x1000000080eL
#define SDB_OSSDUMPSYSINFO                                 0x1000000080fL
#define SDB_OSSMCHCODE                                     0x10000000810L
#define SDB_OSSDUMPSIGINFO                                 0x10000000811L
#define SDB_OSSWLKSTK                                      0x10000000812L
#define SDB_OSSGETSYMBNFA                                  0x10000000813L
#define SDB_OSSDUMPREGSINFO                                0x10000000814L
#define SDB_OSSDUMPST                                      0x10000000815L
#define SDB_OSSDUMPREGSINFO2                               0x10000000816L
#define SDB_OSSDUMPST2                                     0x10000000817L
#define SDB_OSSDUMPREGSINFO3                               0x10000000818L
#define SDB_OSSDUMPST3                                     0x10000000819L
#define SDB_OSSSYMINIT                                     0x1000000081aL
#define SDB_OSSWKSEX                                       0x1000000081bL
#define SDB_OSSWS                                          0x1000000081cL
#define SDB_OSSGETSYMBNFADDR                               0x1000000081dL
#define SDB__OSSSK__OSSSK                                  0x1000000081eL
#define SDB__OSSSK__OSSSK2                                 0x1000000081fL
#define SDB__OSSSK__OSSSK3                                 0x10000000820L
#define SDB_OSSSK_INITTSK                                  0x10000000821L
#define SDB_OSSSK_SETSKLI                                  0x10000000822L
#define SDB_OSSSK_BIND_LSTN                                0x10000000823L
#define SDB_OSSSK_SEND                                     0x10000000824L
#define SDB_OSSSK_ISCONN                                   0x10000000825L
#define SDB_OSSSK_CONNECT                                  0x10000000826L
#define SDB_OSSSK_CLOSE                                    0x10000000827L
#define SDB_OSSSK_DISNAG                                   0x10000000828L
#define SDB_OSSSK_SECURE                                   0x10000000829L
#define SDB_OSSSK_DOSSLHANDSHAKE                           0x1000000082aL
#define SDB_OSSSK__GETADDR                                 0x1000000082bL
#define SDB_OSSSK_SETTMOUT                                 0x1000000082cL
#define SDB__OSSSK__COMPLETE                               0x1000000082dL
#define SDB_OSSMODULEHANDLE_INIT                           0x1000000082eL
#define SDB_OSSMODULEHANDLE_UNLOAD                         0x1000000082fL
#define SDB_OSSMODULEHANDLE_RESOLVEADDRESS                 0x10000000830L
#define SDB__OSSAIOMSGPROC__PROC                           0x10000000831L
#define SDB__OSSAIOMSGPROC__HNDWP                          0x10000000832L
#define SDB__OSSAIOMSGPROC__HNDRPH                         0x10000000833L
#define SDB__OSSAIOMSGPROC__RDPH                           0x10000000834L
#define SDB__OSSAIOMSGPROC_CONNECT                         0x10000000835L
#define SDB__TMPAIR_CHK_DLINE                              0x10000000836L
#define SDB__TMPAIR_RUN                                    0x10000000837L
#define SDB__OSSAIO__HNDAPT                                0x10000000838L
#define SDB__OSSAIO__ACCEPT                                0x10000000839L
#define SDB__OSSAIO_CONNECT                                0x1000000083aL
#define SDB__OSSAIO_ADDTIMER                               0x1000000083bL
#define SDB__OSSAIO_RMTIMER                                0x1000000083cL
#define SDB__OSSMMF_OPEN                                   0x1000000083dL
#define SDB__OSSMMF_CLOSE                                  0x1000000083eL
#define SDB__OSSMMF_SIZE                                   0x1000000083fL
#define SDB__OSSMMF_MAP                                    0x10000000840L
#define SDB__OSSMMF_FLHALL                                 0x10000000841L
#define SDB__OSSMMF_FLUSH                                  0x10000000842L
#define SDB__OSSMMF_UNLINK                                 0x10000000843L
#define SDB__OSSMEMALLOC                                   0x10000000844L
#define SDB__OSSMEMREALLOC                                 0x10000000845L
#define SDB__OSSMEMFREE                                    0x10000000846L
#define SDB_OSSISPROCRUNNING                               0x10000000847L
#define SDB_OSSWAITCHLD                                    0x10000000848L
#define SDB_OSSCRTLST                                      0x10000000849L
#define SDB_OSSEXEC2                                       0x1000000084aL
#define SDB_OSSEXEC                                        0x1000000084bL
#define SDB_OSSENBNMCHGS                                   0x1000000084cL
#define SDB_OSSRENMPROC                                    0x1000000084dL
#define SDB_OSSVERIFYPID                                   0x1000000084eL
#define SDB_OSSRSVPATH                                     0x1000000084fL
#define SDB_OSSWTINT                                       0x10000000850L
#define SDB_OSSSTARTSERVICE                                0x10000000851L
#define SDB_OSS_WFSTRS                                     0x10000000852L
#define SDB_OSS_STOPSERVICE                                0x10000000853L
#define SDB_OSSCRTPADUPHND                                 0x10000000854L
#define SDB_WIN_OSSEXEC                                    0x10000000855L
#define SDB_OSSGETEWD                                      0x10000000856L
#define SDB_OSSCMSTART_BLDARGS                             0x10000000857L
#define SDB_OSS_STARTPROCESS                               0x10000000858L
#define SDB_OSSTS2STR                                      0x10000000859L
#define SDB_OSSGETCPUUSG                                   0x1000000085aL
#define SDB_OSSGETCPUUSG2                                  0x1000000085bL
#define SDB_OSSTCF_INIT                                    0x1000000085cL
#define SDB_OSSSRAND                                       0x1000000085dL
#define SDB_OSSRAND                                        0x1000000085eL
#define SDB_OSSHEXDL                                       0x1000000085fL
#define SDB_OSSHEXDUMPBUF                                  0x10000000860L
#define SDB_OSSGETMEMINFO                                  0x10000000861L
#define SDB_OSSGETDISKINFO                                 0x10000000862L
#define SDB_OSSGETCPUINFO                                  0x10000000863L
#define SDB_OSSGETPROCMEMINFO                              0x10000000864L
#define SDB__OSSEVN_WAIT                                   0x10000000865L
#define SDB__OSSEN_SIGNAL                                  0x10000000866L
#define SDB__OSSEN_SIGALL                                  0x10000000867L
#define SDB__OSSVN_RESET                                   0x10000000868L
#define SDB__OSSRWM_LOCK_R                                 0x10000000869L
#define SDB__OSSRWM_LOCK_W                                 0x1000000086aL
#define SDB__OSSRWM_RLS_R                                  0x1000000086bL
#define SDB__OSSRWM_RLS_W                                  0x1000000086cL
#define SDB_OSSPFOP_OPEN                                   0x1000000086dL
#define SDB_OSSPFOP_READ                                   0x1000000086eL
#define SDB_OSSPFOP_WRITE                                  0x1000000086fL
#define SDB_OSSPFOP_FWRITE                                 0x10000000870L
#define SDB_OSSPFOP_GETSIZE                                0x10000000871L
#define SDB_OSSNTHND                                       0x10000000872L
#define SDB_OSSST                                          0x10000000873L
#define SDB_OSSEDUCTHND                                    0x10000000874L
#define SDB_OSSEDUEXCFLT                                   0x10000000875L
#define SDB_OSSDMPSYSTM                                    0x10000000876L
#define SDB_OSSDMPDBINFO                                   0x10000000877L
#define SDB_OSSSTKTRA                                      0x10000000878L
#define SDB_OSSREGSIGHND                                   0x10000000879L
#define SDB__OSSENUMNMPS                                   0x1000000087aL
#define SDB__OSSENUMNMPS2                                  0x1000000087bL
#define SDB_OSSCRTNMP                                      0x1000000087cL
#define SDB_OSSOPENNMP                                     0x1000000087dL
#define SDB_OSSCONNNMP                                     0x1000000087eL
#define SDB_OSSRENMP                                       0x1000000087fL
#define SDB_OSSWTNMP                                       0x10000000880L
#define SDB_OSSDISCONNNMP                                  0x10000000881L
#define SDB_OSSCLSNMP                                      0x10000000882L
#define SDB_OSSNMP2FD                                      0x10000000883L
#define SDB_OSSCRTNP                                       0x10000000884L
#define SDB_OSSOPENNP                                      0x10000000885L
#define SDB_OSSRDNP                                        0x10000000886L
#define SDB__OSSWTNP                                       0x10000000887L
#define SDB_OSSDELNP                                       0x10000000888L
#define SDB_OSSNP2FD                                       0x10000000889L
#define SDB_OSSCLNPBYNM                                    0x1000000088aL
#define SDB_GETEXECNM                                      0x1000000088bL
#define SDB_OSSLCEXEC                                      0x1000000088cL
#define SDB_OSSOPEN                                        0x1000000088dL
#define SDB_OSSCLOSE                                       0x1000000088eL
#define SDB_OSSMKDIR                                       0x1000000088fL
#define SDB_OSSDELETE                                      0x10000000890L
#define SDB_OSSFILECOPY                                    0x10000000891L
#define SDB_OSSACCESS                                      0x10000000892L
#define SDB_OSSREAD                                        0x10000000893L
#define SDB_OSSWRITE                                       0x10000000894L
#define SDB_OSSSEEK                                        0x10000000895L
#define SDB_OSSSEEKANDREAD                                 0x10000000896L
#define SDB_OSSSEEKANDWRITE                                0x10000000897L
#define SDB_OSSFSYNC                                       0x10000000898L
#define SDB_OSSGETPATHTYPE                                 0x10000000899L
#define SDB_OSSGETFSBYNM                                   0x1000000089aL
#define SDB_OSSGETFILESIZE                                 0x1000000089bL
#define SDB_OSSTRUNCATEFILE                                0x1000000089cL
#define SDB_OSSEXTFILE                                     0x1000000089dL
#define SDB_OSSGETREALPATH                                 0x1000000089eL
#define SDB_OSSGETFSTYPE                                   0x1000000089fL
#define SDB_OSSRENMPATH                                    0x100000008a0L
#define SDB_OSSLOCKFILE                                    0x100000008a1L
#endif
