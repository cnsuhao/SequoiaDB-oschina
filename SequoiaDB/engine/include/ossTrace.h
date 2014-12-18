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
#ifndef ossTRACE_H__
#define ossTRACE_H__
#define SDB_OSSTS2STR                                      0x1000000029cL
#define SDB_OSSGETCPUUSG                                   0x1000000029dL
#define SDB_OSSGETCPUUSG2                                  0x1000000029eL
#define SDB_OSSTCF_INIT                                    0x1000000029fL
#define SDB_OSSSRAND                                       0x100000002a0L
#define SDB_OSSRAND                                        0x100000002a1L
#define SDB_OSSHEXDL                                       0x100000002a2L
#define SDB_OSSHEXDUMPBUF                                  0x100000002a3L
#define SDB_OSSGETMEMINFO                                  0x100000002a4L
#define SDB_OSSGETDISKINFO                                 0x100000002a5L
#define SDB_OSSGETCPUINFO                                  0x100000002a6L
#define SDB_OSSGETPROCMEMINFO                              0x100000002a7L
#define SDB_OSSOPEN                                        0x100000002a8L
#define SDB_OSSCLOSE                                       0x100000002a9L
#define SDB_OSSMKDIR                                       0x100000002aaL
#define SDB_OSSDELETE                                      0x100000002abL
#define SDB_OSSFILECOPY                                    0x100000002acL
#define SDB_OSSACCESS                                      0x100000002adL
#define SDB_OSSREAD                                        0x100000002aeL
#define SDB_OSSWRITE                                       0x100000002afL
#define SDB_OSSSEEK                                        0x100000002b0L
#define SDB_OSSSEEKANDREAD                                 0x100000002b1L
#define SDB_OSSSEEKANDWRITE                                0x100000002b2L
#define SDB_OSSFSYNC                                       0x100000002b3L
#define SDB_OSSGETPATHTYPE                                 0x100000002b4L
#define SDB_OSSGETFSBYNM                                   0x100000002b5L
#define SDB_OSSGETFILESIZE                                 0x100000002b6L
#define SDB_OSSTRUNCATEFILE                                0x100000002b7L
#define SDB_OSSEXTFILE                                     0x100000002b8L
#define SDB_OSSGETREALPATH                                 0x100000002b9L
#define SDB_OSSGETFSTYPE                                   0x100000002baL
#define SDB_OSSRENMPATH                                    0x100000002bbL
#define SDB_OSSLOCKFILE                                    0x100000002bcL
#define SDB__OSSRWM_LOCK_R                                 0x100000002bdL
#define SDB__OSSRWM_LOCK_W                                 0x100000002beL
#define SDB__OSSRWM_RLS_R                                  0x100000002bfL
#define SDB__OSSRWM_RLS_W                                  0x100000002c0L
#define SDB_OSSPFOP_OPEN                                   0x100000002c1L
#define SDB_OSSPFOP_READ                                   0x100000002c2L
#define SDB_OSSPFOP_WRITE                                  0x100000002c3L
#define SDB_OSSPFOP_FWRITE                                 0x100000002c4L
#define SDB_OSSPFOP_GETSIZE                                0x100000002c5L
#define SDB_GETEXECNM                                      0x100000002c6L
#define SDB_OSSLCEXEC                                      0x100000002c7L
#define SDB__OSSSK__OSSSK                                  0x100000002c8L
#define SDB__OSSSK__OSSSK2                                 0x100000002c9L
#define SDB__OSSSK__OSSSK3                                 0x100000002caL
#define SDB_OSSSK_INITTSK                                  0x100000002cbL
#define SDB_OSSSK_SETSKLI                                  0x100000002ccL
#define SDB_OSSSK_BIND_LSTN                                0x100000002cdL
#define SDB_OSSSK_SEND                                     0x100000002ceL
#define SDB_OSSSK_ISCONN                                   0x100000002cfL
#define SDB_OSSSK_CONNECT                                  0x100000002d0L
#define SDB_OSSSK_CLOSE                                    0x100000002d1L
#define SDB_OSSSK_DISNAG                                   0x100000002d2L
#define SDB_OSSSK__GETADDR                                 0x100000002d3L
#define SDB_OSSSK_SETTMOUT                                 0x100000002d4L
#define SDB__OSSSK__COMPLETE                               0x100000002d5L
#define SDB_OSSMODULEHANDLE_INIT                           0x100000002d6L
#define SDB_OSSMODULEHANDLE_UNLOAD                         0x100000002d7L
#define SDB_OSSMODULEHANDLE_RESOLVEADDRESS                 0x100000002d8L
#define SDB_OSSISPROCRUNNING                               0x100000002d9L
#define SDB_OSSWAITCHLD                                    0x100000002daL
#define SDB_OSSCRTLST                                      0x100000002dbL
#define SDB_OSSEXEC2                                       0x100000002dcL
#define SDB_OSSEXEC                                        0x100000002ddL
#define SDB_OSSENBNMCHGS                                   0x100000002deL
#define SDB_OSSRENMPROC                                    0x100000002dfL
#define SDB_OSSVERIFYPID                                   0x100000002e0L
#define SDB_OSSRSVPATH                                     0x100000002e1L
#define SDB_OSSWTINT                                       0x100000002e2L
#define SDB_OSSSTARTSERVICE                                0x100000002e3L
#define SDB_OSS_WFSTRS                                     0x100000002e4L
#define SDB_OSS_STOPSERVICE                                0x100000002e5L
#define SDB_OSSCRTPADUPHND                                 0x100000002e6L
#define SDB_WIN_OSSEXEC                                    0x100000002e7L
#define SDB_OSSGETEWD                                      0x100000002e8L
#define SDB_OSSCMSTART_BLDARGS                             0x100000002e9L
#define SDB_OSS_STARTPROCESS                               0x100000002eaL
#define SDB__OSSMMF_OPEN                                   0x100000002ebL
#define SDB__OSSMMF_CLOSE                                  0x100000002ecL
#define SDB__OSSMMF_SIZE                                   0x100000002edL
#define SDB__OSSMMF_MAP                                    0x100000002eeL
#define SDB__OSSMMF_FLHALL                                 0x100000002efL
#define SDB__OSSMMF_FLUSH                                  0x100000002f0L
#define SDB__OSSMMF_UNLINK                                 0x100000002f1L
#define SDB__OSSMEMALLOC                                   0x100000002f2L
#define SDB__OSSMEMREALLOC                                 0x100000002f3L
#define SDB__OSSMEMFREE                                    0x100000002f4L
#define SDB_OSSRSTSYSSIG                                   0x100000002f5L
#define SDB_OSSSIGHNDABT                                   0x100000002f6L
#define SDB_OSSFUNCADDR2NM                                 0x100000002f7L
#define SDB_OSSDUMPSYSTM                                   0x100000002f8L
#define SDB_OSSDUMPDBINFO                                  0x100000002f9L
#define SDB_OSSDUMPSYSINFO                                 0x100000002faL
#define SDB_OSSMCHCODE                                     0x100000002fbL
#define SDB_OSSDUMPSIGINFO                                 0x100000002fcL
#define SDB_OSSWLKSTK                                      0x100000002fdL
#define SDB_OSSGETSYMBNFA                                  0x100000002feL
#define SDB_OSSDUMPREGSINFO                                0x100000002ffL
#define SDB_OSSDUMPST                                      0x10000000300L
#define SDB_OSSDUMPREGSINFO2                               0x10000000301L
#define SDB_OSSDUMPST2                                     0x10000000302L
#define SDB_OSSDUMPREGSINFO3                               0x10000000303L
#define SDB_OSSDUMPST3                                     0x10000000304L
#define SDB_OSSSYMINIT                                     0x10000000305L
#define SDB_OSSWKSEX                                       0x10000000306L
#define SDB_OSSWS                                          0x10000000307L
#define SDB_OSSGETSYMBNFADDR                               0x10000000308L
#define SDB__OSSEVN_WAIT                                   0x10000000309L
#define SDB__OSSEN_SIGNAL                                  0x1000000030aL
#define SDB__OSSEN_SIGALL                                  0x1000000030bL
#define SDB__OSSVN_RESET                                   0x1000000030cL
#define SDB_OSSNTHND                                       0x1000000030dL
#define SDB_OSSST                                          0x1000000030eL
#define SDB_OSSEDUCTHND                                    0x1000000030fL
#define SDB_OSSEDUEXCFLT                                   0x10000000310L
#define SDB_OSSDMPSYSTM                                    0x10000000311L
#define SDB_OSSDMPDBINFO                                   0x10000000312L
#define SDB_OSSSTKTRA                                      0x10000000313L
#define SDB_OSSREGSIGHND                                   0x10000000314L
#define SDB__OSSAIOMSGPROC__PROC                           0x10000000315L
#define SDB__OSSAIOMSGPROC__HNDWP                          0x10000000316L
#define SDB__OSSAIOMSGPROC__HNDRPH                         0x10000000317L
#define SDB__OSSAIOMSGPROC__RDPH                           0x10000000318L
#define SDB__OSSAIOMSGPROC_CONNECT                         0x10000000319L
#define SDB__TMPAIR_CHK_DLINE                              0x1000000031aL
#define SDB__TMPAIR_RUN                                    0x1000000031bL
#define SDB__OSSAIO__HNDAPT                                0x1000000031cL
#define SDB__OSSAIO__ACCEPT                                0x1000000031dL
#define SDB__OSSAIO_CONNECT                                0x1000000031eL
#define SDB__OSSAIO_ADDTIMER                               0x1000000031fL
#define SDB__OSSAIO_RMTIMER                                0x10000000320L
#define SDB__OSSENUMNMPS                                   0x10000000321L
#define SDB__OSSENUMNMPS2                                  0x10000000322L
#define SDB_OSSCRTNMP                                      0x10000000323L
#define SDB_OSSOPENNMP                                     0x10000000324L
#define SDB_OSSCONNNMP                                     0x10000000325L
#define SDB_OSSRENMP                                       0x10000000326L
#define SDB_OSSWTNMP                                       0x10000000327L
#define SDB_OSSDISCONNNMP                                  0x10000000328L
#define SDB_OSSCLSNMP                                      0x10000000329L
#define SDB_OSSNMP2FD                                      0x1000000032aL
#define SDB_OSSCRTNP                                       0x1000000032bL
#define SDB_OSSOPENNP                                      0x1000000032cL
#define SDB_OSSRDNP                                        0x1000000032dL
#define SDB__OSSWTNP                                       0x1000000032eL
#define SDB_OSSDELNP                                       0x1000000032fL
#define SDB_OSSNP2FD                                       0x10000000330L
#define SDB_OSSCLNPBYNM                                    0x10000000331L
#endif
