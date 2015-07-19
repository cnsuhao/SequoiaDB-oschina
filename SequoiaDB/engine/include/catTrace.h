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
#ifndef catTRACE_H__
#define catTRACE_H__
#define SDB_CATALOGCB_INIT                                 0x400000008L
#define SDB_CATALOGCB_INSERTGROUPID                        0x400000009L
#define SDB_CATALOGCB_REMOVEGROUPID                        0x40000000aL
#define SDB_CATALOGCB_ACTIVEGROUP                          0x40000000bL
#define SDB_CATALOGCB_INSERTNODEID                         0x40000000cL
#define SDB_CATALOGCB_GETAGROUPRAND                        0x40000000dL
#define SDB_CATALOGCB_ALLOCGROUPID                         0x40000000eL
#define SDB_CATALOGCB_ALLOCCATANODEID                      0x40000000fL
#define SDB_CATALOGCB_ALLOCNODEID                          0x400000010L
#define SDB_CATALOGCB_UPDATEROUTEID                        0x400000011L
#define SDB_CATALOGMGR_DROPCS                              0x400000012L
#define SDB_CATALOGMGR_CRT_PROCEDURES                      0x400000013L
#define SDB_CATALOGMGR_RM_PROCEDURES                       0x400000014L
#define SDB_CATALOGMGR_QUERYSPACEINFO                      0x400000015L
#define SDB_CATALOGMGR_QUERYCATALOG                        0x400000016L
#define SDB_CATALOGMGR_DROPCOLLECTION                      0x400000017L
#define SDB_CATALOGMGR_QUERYTASK                           0x400000018L
#define SDB_CATALOGMGR_ALTERCOLLECTION                     0x400000019L
#define SDB_CATALOGMGR_CREATECS                            0x40000001aL
#define SDB_CATALOGMGR_CREATECL                            0x40000001bL
#define SDB_CATALOGMGR_CMDSPLIT                            0x40000001cL
#define SDB_CATALOGMGR__CHECKCSOBJ                         0x40000001dL
#define SDB_CATALOGMGR__CHECKANDBUILDCATARECORD            0x40000001eL
#define SDB_CATALOGMGR__ASSIGNGROUP                        0x40000001fL
#define SDB_CATALOGMGR__CHECKGROUPINDOMAIN                 0x400000020L
#define SDB_CATALOGMGR__CREATECS                           0x400000021L
#define SDB_CATALOGMGR_CREATECOLLECTION                    0x400000022L
#define SDB_CATALOGMGR_BUILDCATALOGRECORD                  0x400000023L
#define SDB_CATALOGMGR_BUILDINITBOUND                      0x400000024L
#define SDB_CATALOGMGR_PROCESSMSG                          0x400000025L
#define SDB_CATALOGMGR_PROCESSCOMMANDMSG                   0x400000026L
#define SDB_CATALOGMGR__BUILDHASHBOUND                     0x400000027L
#define SDB_CATALOGMGR_CMDLINKCOLLECTION                   0x400000028L
#define SDB_CATALOGMGR_CMDUNLINKCOLLECTION                 0x400000029L
#define SDB_CATALOGMGR_CREATEDOMAIN                        0x40000002aL
#define SDB_CATALOGMGR_DROPDOMAIN                          0x40000002bL
#define SDB_CATALOGMGR_ALTERDOMAIN                         0x40000002cL
#define SDB_CATALOGMGR__BUILDALTERGROUPS                   0x40000002dL
#define SDB_CATALOGMGR__CHOOSEFGROUPOFCL                   0x40000002eL
#define SDB_CATALOGMGR_AUTOHASHSPLIT                       0x40000002fL
#define SDB_CATALOGMGR__COMBINEOPTIONS                     0x400000030L
#define SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ        0x400000031L
#define SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS             0x400000032L
#define SDB_CATGROUPNAMEVALIDATE                           0x400000033L
#define SDB_CATDOMAINOPTIONSEXTRACT                        0x400000034L
#define SDB_CATRESOLVECOLLECTIONNAME                       0x400000035L
#define SDB_CATQUERYANDGETMORE                             0x400000036L
#define SDB_CATGETONEOBJ                                   0x400000037L
#define SDB_CATGETGROUPOBJ                                 0x400000038L
#define SDB_CATGETGROUPOBJ1                                0x400000039L
#define SDB_CATGETGROUPOBJ2                                0x40000003aL
#define SDB_CATGROUPCHECK                                  0x40000003bL
#define SDB_CATSERVICECHECK                                0x40000003cL
#define SDB_CATGROUPID2NAME                                0x40000003dL
#define SDB_CATGROUPNAME2ID                                0x40000003eL
#define SDB_CATGETDOMAINOBJ                                0x40000003fL
#define SDB_CATDOMAINCHECK                                 0x400000040L
#define SDB_CATGETDOMAINGROUPS                             0x400000041L
#define SDB_CATGETDOMAINGROUPS1                            0x400000042L
#define SDB_CATADDGRP2DOMAIN                               0x400000043L
#define SDB_CATDELGRPFROMDOMAIN                            0x400000044L
#define SDB_CAATADDCL2CS                                   0x400000045L
#define SDB_CATDELCLFROMCS                                 0x400000046L
#define SDB_CATRESTORECS                                   0x400000047L
#define SDB_CATCHECKSPACEEXIST                             0x400000048L
#define SDB_CATREMOVECL                                    0x400000049L
#define SDB_CATCHECKCOLLECTIONEXIST                        0x40000004aL
#define SDB_CATUPDATECATALOG                               0x40000004bL
#define SDB_CATADDTASK                                     0x40000004cL
#define SDB_CATGETTASK                                     0x40000004dL
#define SDB_CATGETTASKSTATUS                               0x40000004eL
#define SDB_CATGETMAXTASKID                                0x40000004fL
#define SDB_CATUPDATETASKSTATUS                            0x400000050L
#define SDB_CATREMOVETASK                                  0x400000051L
#define SDB_CATREMOVETASK1                                 0x400000052L
#define SDB_CATREMOVECLEX                                  0x400000053L
#define SDB_CATREMOVECSEX                                  0x400000054L
#define SDB_CATPRASEFUNC                                   0x400000055L
#define SDB_CATLINKCL                                      0x400000056L
#define SDB_CATUNLINKCL                                    0x400000057L
#define SDB_CATMAINCT_HANDLEMSG                            0x400000058L
#define SDB_CATMAINCT_POSTMSG                              0x400000059L
#define SDB_CATMAINCT_INIT                                 0x40000005aL
#define SDB_CATMAINCT__CREATESYSIDX                        0x40000005bL
#define SDB_CATMAINCT__CREATESYSCOL                        0x40000005cL
#define SDB_CATMAINCT__ENSUREMETADATA                      0x40000005dL
#define SDB_CATMAINCT_ACTIVE                               0x40000005eL
#define SDB_CATMAINCT_DEACTIVE                             0x40000005fL
#define SDB_CATMAINCT_BUILDMSGEVENT                        0x400000060L
#define SDB_CATMAINCT_GETMOREMSG                           0x400000061L
#define SDB_CATMAINCT_KILLCONTEXT                          0x400000062L
#define SDB_CATMAINCT_QUERYMSG                             0x400000063L
#define SDB_CATMAINCT_QUERYREQUEST                         0x400000064L
#define SDB_CATMAINCT_AUTHCRT                              0x400000065L
#define SDB_CATMAINCT_AUTHENTICATE                         0x400000066L
#define SDB_CATMAINCT_AUTHDEL                              0x400000067L
#define SDB_CATMAINCT_CHECKROUTEID                         0x400000068L
#define SDB_CATNODEMGR_INIT                                0x400000069L
#define SDB_CATNODEMGR_ACTIVE                              0x40000006aL
#define SDB_CATNODEMGR_DEACTIVE                            0x40000006bL
#define SDB_CATNODEMGR_PROCESSMSG                          0x40000006cL
#define SDB_CATNODEMGR_PRIMARYCHANGE                       0x40000006dL
#define SDB_CATNODEMGR_GRPREQ                              0x40000006eL
#define SDB_CATNODEMGR_REGREQ                              0x40000006fL
#define SDB_CATNODEMGR_PCREATEGRP                          0x400000070L
#define SDB_CATNODEMGR_CREATENODE                          0x400000071L
#define SDB_CATNODEMGR_UPDATENODE                          0x400000072L
#define SDB_CATNODEMGR_DELNODE                             0x400000073L
#define SDB_CATNODEMGR_PREMOVEGRP                          0x400000074L
#define SDB_CATNODEMGR_ACTIVEGRP                           0x400000075L
#define SDB_CATNODEMGR_READCATACONF                        0x400000076L
#define SDB_CATNODEMGR_PARSECATCONF                        0x400000077L
#define SDB_CATNODEMGR_SAVEGRPINFO                         0x400000078L
#define SDB_CATNODEMGR_GENGROUPINFO                        0x400000079L
#define SDB_CATNODEMGR_GETNODEINFOBYCONF                   0x40000007aL
#define SDB_CATNODEMGR_PARSELINE                           0x40000007bL
#define SDB_CATNODEMGR_PARSEIDINFO                         0x40000007cL
#define SDB_CATNODEMGR_GETNODEINFO                         0x40000007dL
#define SDB_CATNODEMGR_CREATEGRP                           0x40000007eL
#define SDB_CATNODEMGR_REMOVEGRP                           0x40000007fL
#endif
