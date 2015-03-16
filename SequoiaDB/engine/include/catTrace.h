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
#define SDB_CATALOGCB_INIT                                 0x400000175L
#define SDB_CATALOGCB_INSERTGROUPID                        0x400000176L
#define SDB_CATALOGCB_REMOVEGROUPID                        0x400000177L
#define SDB_CATALOGCB_ACTIVEGROUP                          0x400000178L
#define SDB_CATALOGCB_INSERTNODEID                         0x400000179L
#define SDB_CATALOGCB_GETAGROUPRAND                        0x40000017aL
#define SDB_CATALOGCB_ALLOCGROUPID                         0x40000017bL
#define SDB_CATALOGCB_ALLOCCATANODEID                      0x40000017cL
#define SDB_CATALOGCB_ALLOCNODEID                          0x40000017dL
#define SDB_CATALOGCB_UPDATEROUTEID                        0x40000017eL
#define SDB_CATALOGMGR_DROPCS                              0x40000017fL
#define SDB_CATALOGMGR_CRT_PROCEDURES                      0x400000180L
#define SDB_CATALOGMGR_RM_PROCEDURES                       0x400000181L
#define SDB_CATALOGMGR_QUERYSPACEINFO                      0x400000182L
#define SDB_CATALOGMGR_QUERYCATALOG                        0x400000183L
#define SDB_CATALOGMGR_DROPCOLLECTION                      0x400000184L
#define SDB_CATALOGMGR_QUERYTASK                           0x400000185L
#define SDB_CATALOGMGR_ALTERCOLLECTION                     0x400000186L
#define SDB_CATALOGMGR_CREATECS                            0x400000187L
#define SDB_CATALOGMGR_CREATECL                            0x400000188L
#define SDB_CATALOGMGR_CMDSPLIT                            0x400000189L
#define SDB_CATALOGMGR__CHECKCSOBJ                         0x40000018aL
#define SDB_CATALOGMGR__CHECKANDBUILDCATARECORD            0x40000018bL
#define SDB_CATALOGMGR__ASSIGNGROUP                        0x40000018cL
#define SDB_CATALOGMGR__CHECKGROUPINDOMAIN                 0x40000018dL
#define SDB_CATALOGMGR__CREATECS                           0x40000018eL
#define SDB_CATALOGMGR_CREATECOLLECTION                    0x40000018fL
#define SDB_CATALOGMGR_BUILDCATALOGRECORD                  0x400000190L
#define SDB_CATALOGMGR_BUILDINITBOUND                      0x400000191L
#define SDB_CATALOGMGR_PROCESSMSG                          0x400000192L
#define SDB_CATALOGMGR_PROCESSCOMMANDMSG                   0x400000193L
#define SDB_CATALOGMGR__BUILDHASHBOUND                     0x400000194L
#define SDB_CATALOGMGR_CMDLINKCOLLECTION                   0x400000195L
#define SDB_CATALOGMGR_CMDUNLINKCOLLECTION                 0x400000196L
#define SDB_CATALOGMGR_CREATEDOMAIN                        0x400000197L
#define SDB_CATALOGMGR_DROPDOMAIN                          0x400000198L
#define SDB_CATALOGMGR_ALTERDOMAIN                         0x400000199L
#define SDB_CATALOGMGR__BUILDALTERGROUPS                   0x40000019aL
#define SDB_CATALOGMGR__CHOOSEFGROUPOFCL                   0x40000019bL
#define SDB_CATALOGMGR_AUTOHASHSPLIT                       0x40000019cL
#define SDB_CATALOGMGR__COMBINEOPTIONS                     0x40000019dL
#define SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ        0x40000019eL
#define SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS             0x40000019fL
#define SDB_CATGROUPNAMEVALIDATE                           0x4000001a0L
#define SDB_CATDOMAINOPTIONSEXTRACT                        0x4000001a1L
#define SDB_CATRESOLVECOLLECTIONNAME                       0x4000001a2L
#define SDB_CATQUERYANDGETMORE                             0x4000001a3L
#define SDB_CATGETONEOBJ                                   0x4000001a4L
#define SDB_CATGETGROUPOBJ                                 0x4000001a5L
#define SDB_CATGETGROUPOBJ1                                0x4000001a6L
#define SDB_CATGETGROUPOBJ2                                0x4000001a7L
#define SDB_CATGROUPCHECK                                  0x4000001a8L
#define SDB_CATSERVICECHECK                                0x4000001a9L
#define SDB_CATGROUPID2NAME                                0x4000001aaL
#define SDB_CATGROUPNAME2ID                                0x4000001abL
#define SDB_CATGETDOMAINOBJ                                0x4000001acL
#define SDB_CATDOMAINCHECK                                 0x4000001adL
#define SDB_CATGETDOMAINGROUPS                             0x4000001aeL
#define SDB_CATGETDOMAINGROUPS1                            0x4000001afL
#define SDB_CATADDGRP2DOMAIN                               0x4000001b0L
#define SDB_CATDELGRPFROMDOMAIN                            0x4000001b1L
#define SDB_CAATADDCL2CS                                   0x4000001b2L
#define SDB_CATDELCLFROMCS                                 0x4000001b3L
#define SDB_CATRESTORECS                                   0x4000001b4L
#define SDB_CATCHECKSPACEEXIST                             0x4000001b5L
#define SDB_CATREMOVECL                                    0x4000001b6L
#define SDB_CATCHECKCOLLECTIONEXIST                        0x4000001b7L
#define SDB_CATUPDATECATALOG                               0x4000001b8L
#define SDB_CATADDTASK                                     0x4000001b9L
#define SDB_CATGETTASK                                     0x4000001baL
#define SDB_CATGETTASKSTATUS                               0x4000001bbL
#define SDB_CATGETMAXTASKID                                0x4000001bcL
#define SDB_CATUPDATETASKSTATUS                            0x4000001bdL
#define SDB_CATREMOVETASK                                  0x4000001beL
#define SDB_CATREMOVETASK1                                 0x4000001bfL
#define SDB_CATREMOVECLEX                                  0x4000001c0L
#define SDB_CATREMOVECSEX                                  0x4000001c1L
#define SDB_CATPRASEFUNC                                   0x4000001c2L
#define SDB_CATLINKCL                                      0x4000001c3L
#define SDB_CATUNLINKCL                                    0x4000001c4L
#define SDB_CATNODEMGR_INIT                                0x4000001c5L
#define SDB_CATNODEMGR_ACTIVE                              0x4000001c6L
#define SDB_CATNODEMGR_PROCESSMSG                          0x4000001c7L
#define SDB_CATNODEMGR_PRIMARYCHANGE                       0x4000001c8L
#define SDB_CATNODEMGR_GRPREQ                              0x4000001c9L
#define SDB_CATNODEMGR_REGREQ                              0x4000001caL
#define SDB_CATNODEMGR_PCREATEGRP                          0x4000001cbL
#define SDB_CATNODEMGR_CREATENODE                          0x4000001ccL
#define SDB_CATNODEMGR_UPDATENODE                          0x4000001cdL
#define SDB_CATNODEMGR_DELNODE                             0x4000001ceL
#define SDB_CATNODEMGR_PREMOVEGRP                          0x4000001cfL
#define SDB_CATNODEMGR_ACTIVEGRP                           0x4000001d0L
#define SDB_CATNODEMGR_READCATACONF                        0x4000001d1L
#define SDB_CATNODEMGR_PARSECATCONF                        0x4000001d2L
#define SDB_CATNODEMGR_SAVEGRPINFO                         0x4000001d3L
#define SDB_CATNODEMGR_GENGROUPINFO                        0x4000001d4L
#define SDB_CATNODEMGR_GETNODEINFOBYCONF                   0x4000001d5L
#define SDB_CATNODEMGR_PARSELINE                           0x4000001d6L
#define SDB_CATNODEMGR_PARSEIDINFO                         0x4000001d7L
#define SDB_CATNODEMGR_GETNODEINFO                         0x4000001d8L
#define SDB_CATNODEMGR_CREATEGRP                           0x4000001d9L
#define SDB_CATNODEMGR_REMOVEGRP                           0x4000001daL
#define SDB_CATMAINCT_HANDLEMSG                            0x4000001dbL
#define SDB_CATMAINCT_POSTMSG                              0x4000001dcL
#define SDB_CATMAINCT_INIT                                 0x4000001ddL
#define SDB_CATMAINCT__CREATESYSIDX                        0x4000001deL
#define SDB_CATMAINCT__CREATESYSCOL                        0x4000001dfL
#define SDB_CATMAINCT__ENSUREMETADATA                      0x4000001e0L
#define SDB_CATMAINCT_ACTIVE                               0x4000001e1L
#define SDB_CATMAINCT_DEACTIVE                             0x4000001e2L
#define SDB_CATMAINCT_BUILDMSGEVENT                        0x4000001e3L
#define SDB_CATMAINCT_GETMOREMSG                           0x4000001e4L
#define SDB_CATMAINCT_KILLCONTEXT                          0x4000001e5L
#define SDB_CATMAINCT_QUERYMSG                             0x4000001e6L
#define SDB_CATMAINCT_QUERYREQUEST                         0x4000001e7L
#define SDB_CATMAINCT_AUTHCRT                              0x4000001e8L
#define SDB_CATMAINCT_AUTHENTICATE                         0x4000001e9L
#define SDB_CATMAINCT_AUTHDEL                              0x4000001eaL
#define SDB_CATMAINCT_CHECKROUTEID                         0x4000001ebL
#endif
