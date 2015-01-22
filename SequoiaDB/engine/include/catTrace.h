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
#define SDB_CATALOGMGR_DROPCS                              0x400000781L
#define SDB_CATALOGMGR_CRT_PROCEDURES                      0x400000782L
#define SDB_CATALOGMGR_RM_PROCEDURES                       0x400000783L
#define SDB_CATALOGMGR_QUERYSPACEINFO                      0x400000784L
#define SDB_CATALOGMGR_QUERYCATALOG                        0x400000785L
#define SDB_CATALOGMGR_DROPCOLLECTION                      0x400000786L
#define SDB_CATALOGMGR_QUERYTASK                           0x400000787L
#define SDB_CATALOGMGR_ALTERCOLLECTION                     0x400000788L
#define SDB_CATALOGMGR_CREATECS                            0x400000789L
#define SDB_CATALOGMGR_CREATECL                            0x40000078aL
#define SDB_CATALOGMGR_CMDSPLIT                            0x40000078bL
#define SDB_CATALOGMGR__CHECKCSOBJ                         0x40000078cL
#define SDB_CATALOGMGR__CHECKANDBUILDCATARECORD            0x40000078dL
#define SDB_CATALOGMGR__ASSIGNGROUP                        0x40000078eL
#define SDB_CATALOGMGR__CHECKGROUPINDOMAIN                 0x40000078fL
#define SDB_CATALOGMGR__CREATECS                           0x400000790L
#define SDB_CATALOGMGR_CREATECOLLECTION                    0x400000791L
#define SDB_CATALOGMGR_BUILDCATALOGRECORD                  0x400000792L
#define SDB_CATALOGMGR_BUILDINITBOUND                      0x400000793L
#define SDB_CATALOGMGR_PROCESSMSG                          0x400000794L
#define SDB_CATALOGMGR_PROCESSCOMMANDMSG                   0x400000795L
#define SDB_CATALOGMGR__BUILDHASHBOUND                     0x400000796L
#define SDB_CATALOGMGR_CMDLINKCOLLECTION                   0x400000797L
#define SDB_CATALOGMGR_CMDUNLINKCOLLECTION                 0x400000798L
#define SDB_CATALOGMGR_CREATEDOMAIN                        0x400000799L
#define SDB_CATALOGMGR_DROPDOMAIN                          0x40000079aL
#define SDB_CATALOGMGR_ALTERDOMAIN                         0x40000079bL
#define SDB_CATALOGMGR__BUILDALTERGROUPS                   0x40000079cL
#define SDB_CATALOGMGR__CHOOSEFGROUPOFCL                   0x40000079dL
#define SDB_CATALOGMGR_AUTOHASHSPLIT                       0x40000079eL
#define SDB_CATALOGMGR__COMBINEOPTIONS                     0x40000079fL
#define SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ        0x4000007a0L
#define SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS             0x4000007a1L
#define SDB_CATGROUPNAMEVALIDATE                           0x4000007a2L
#define SDB_CATDOMAINOPTIONSEXTRACT                        0x4000007a3L
#define SDB_CATRESOLVECOLLECTIONNAME                       0x4000007a4L
#define SDB_CATQUERYANDGETMORE                             0x4000007a5L
#define SDB_CATGETONEOBJ                                   0x4000007a6L
#define SDB_CATGETGROUPOBJ                                 0x4000007a7L
#define SDB_CATGETGROUPOBJ1                                0x4000007a8L
#define SDB_CATGETGROUPOBJ2                                0x4000007a9L
#define SDB_CATGROUPCHECK                                  0x4000007aaL
#define SDB_CATSERVICECHECK                                0x4000007abL
#define SDB_CATGROUPID2NAME                                0x4000007acL
#define SDB_CATGROUPNAME2ID                                0x4000007adL
#define SDB_CATGETDOMAINOBJ                                0x4000007aeL
#define SDB_CATDOMAINCHECK                                 0x4000007afL
#define SDB_CATGETDOMAINGROUPS                             0x4000007b0L
#define SDB_CATGETDOMAINGROUPS1                            0x4000007b1L
#define SDB_CATADDGRP2DOMAIN                               0x4000007b2L
#define SDB_CATDELGRPFROMDOMAIN                            0x4000007b3L
#define SDB_CAATADDCL2CS                                   0x4000007b4L
#define SDB_CATDELCLFROMCS                                 0x4000007b5L
#define SDB_CATRESTORECS                                   0x4000007b6L
#define SDB_CATCHECKSPACEEXIST                             0x4000007b7L
#define SDB_CATREMOVECL                                    0x4000007b8L
#define SDB_CATCHECKCOLLECTIONEXIST                        0x4000007b9L
#define SDB_CATUPDATECATALOG                               0x4000007baL
#define SDB_CATADDTASK                                     0x4000007bbL
#define SDB_CATGETTASK                                     0x4000007bcL
#define SDB_CATGETTASKSTATUS                               0x4000007bdL
#define SDB_CATGETMAXTASKID                                0x4000007beL
#define SDB_CATUPDATETASKSTATUS                            0x4000007bfL
#define SDB_CATREMOVETASK                                  0x4000007c0L
#define SDB_CATREMOVETASK1                                 0x4000007c1L
#define SDB_CATREMOVECLEX                                  0x4000007c2L
#define SDB_CATREMOVECSEX                                  0x4000007c3L
#define SDB_CATPRASEFUNC                                   0x4000007c4L
#define SDB_CATLINKCL                                      0x4000007c5L
#define SDB_CATUNLINKCL                                    0x4000007c6L
#define SDB_CATNODEMGR_INIT                                0x4000007c7L
#define SDB_CATNODEMGR_ACTIVE                              0x4000007c8L
#define SDB_CATNODEMGR_DEACTIVE                            0x4000007c9L
#define SDB_CATNODEMGR_PROCESSMSG                          0x4000007caL
#define SDB_CATNODEMGR_PRIMARYCHANGE                       0x4000007cbL
#define SDB_CATNODEMGR_GRPREQ                              0x4000007ccL
#define SDB_CATNODEMGR_REGREQ                              0x4000007cdL
#define SDB_CATNODEMGR_PCREATEGRP                          0x4000007ceL
#define SDB_CATNODEMGR_CREATENODE                          0x4000007cfL
#define SDB_CATNODEMGR_UPDATENODE                          0x4000007d0L
#define SDB_CATNODEMGR_DELNODE                             0x4000007d1L
#define SDB_CATNODEMGR_PREMOVEGRP                          0x4000007d2L
#define SDB_CATNODEMGR_ACTIVEGRP                           0x4000007d3L
#define SDB_CATNODEMGR_READCATACONF                        0x4000007d4L
#define SDB_CATNODEMGR_PARSECATCONF                        0x4000007d5L
#define SDB_CATNODEMGR_SAVEGRPINFO                         0x4000007d6L
#define SDB_CATNODEMGR_GENGROUPINFO                        0x4000007d7L
#define SDB_CATNODEMGR_GETNODEINFOBYCONF                   0x4000007d8L
#define SDB_CATNODEMGR_PARSELINE                           0x4000007d9L
#define SDB_CATNODEMGR_PARSEIDINFO                         0x4000007daL
#define SDB_CATNODEMGR_GETNODEINFO                         0x4000007dbL
#define SDB_CATNODEMGR_CREATEGRP                           0x4000007dcL
#define SDB_CATNODEMGR_REMOVEGRP                           0x4000007ddL
#define SDB_CATALOGCB_INIT                                 0x4000007deL
#define SDB_CATALOGCB_INSERTGROUPID                        0x4000007dfL
#define SDB_CATALOGCB_REMOVEGROUPID                        0x4000007e0L
#define SDB_CATALOGCB_ACTIVEGROUP                          0x4000007e1L
#define SDB_CATALOGCB_INSERTNODEID                         0x4000007e2L
#define SDB_CATALOGCB_GETAGROUPRAND                        0x4000007e3L
#define SDB_CATALOGCB_ALLOCGROUPID                         0x4000007e4L
#define SDB_CATALOGCB_ALLOCCATANODEID                      0x4000007e5L
#define SDB_CATALOGCB_ALLOCNODEID                          0x4000007e6L
#define SDB_CATALOGCB_UPDATEROUTEID                        0x4000007e7L
#define SDB_CATMAINCT_HANDLEMSG                            0x4000007e8L
#define SDB_CATMAINCT_POSTMSG                              0x4000007e9L
#define SDB_CATMAINCT_INIT                                 0x4000007eaL
#define SDB_CATMAINCT__CREATESYSIDX                        0x4000007ebL
#define SDB_CATMAINCT__CREATESYSCOL                        0x4000007ecL
#define SDB_CATMAINCT__ENSUREMETADATA                      0x4000007edL
#define SDB_CATMAINCT_ACTIVE                               0x4000007eeL
#define SDB_CATMAINCT_DEACTIVE                             0x4000007efL
#define SDB_CATMAINCT_BUILDMSGEVENT                        0x4000007f0L
#define SDB_CATMAINCT_GETMOREMSG                           0x4000007f1L
#define SDB_CATMAINCT_KILLCONTEXT                          0x4000007f2L
#define SDB_CATMAINCT_QUERYMSG                             0x4000007f3L
#define SDB_CATMAINCT_QUERYREQUEST                         0x4000007f4L
#define SDB_CATMAINCT_AUTHCRT                              0x4000007f5L
#define SDB_CATMAINCT_AUTHENTICATE                         0x4000007f6L
#define SDB_CATMAINCT_AUTHDEL                              0x4000007f7L
#define SDB_CATMAINCT_CHECKROUTEID                         0x4000007f8L
#endif
