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
#define SDB_CATALOGMGR_DROPCS                              0x4000005e4L
#define SDB_CATALOGMGR_CRT_PROCEDURES                      0x4000005e5L
#define SDB_CATALOGMGR_RM_PROCEDURES                       0x4000005e6L
#define SDB_CATALOGMGR_QUERYSPACEINFO                      0x4000005e7L
#define SDB_CATALOGMGR_QUERYCATALOG                        0x4000005e8L
#define SDB_CATALOGMGR_DROPCOLLECTION                      0x4000005e9L
#define SDB_CATALOGMGR_QUERYTASK                           0x4000005eaL
#define SDB_CATALOGMGR_ALTERCOLLECTION                     0x4000005ebL
#define SDB_CATALOGMGR_CREATECS                            0x4000005ecL
#define SDB_CATALOGMGR_CREATECL                            0x4000005edL
#define SDB_CATALOGMGR_CMDSPLIT                            0x4000005eeL
#define SDB_CATALOGMGR__CHECKCSOBJ                         0x4000005efL
#define SDB_CATALOGMGR__CHECKANDBUILDCATARECORD            0x4000005f0L
#define SDB_CATALOGMGR__ASSIGNGROUP                        0x4000005f1L
#define SDB_CATALOGMGR__CHECKGROUPINDOMAIN                 0x4000005f2L
#define SDB_CATALOGMGR__CREATECS                           0x4000005f3L
#define SDB_CATALOGMGR_CREATECOLLECTION                    0x4000005f4L
#define SDB_CATALOGMGR_BUILDCATALOGRECORD                  0x4000005f5L
#define SDB_CATALOGMGR_BUILDINITBOUND                      0x4000005f6L
#define SDB_CATALOGMGR_PROCESSMSG                          0x4000005f7L
#define SDB_CATALOGMGR_PROCESSCOMMANDMSG                   0x4000005f8L
#define SDB_CATALOGMGR__BUILDHASHBOUND                     0x4000005f9L
#define SDB_CATALOGMGR_CMDLINKCOLLECTION                   0x4000005faL
#define SDB_CATALOGMGR_CMDUNLINKCOLLECTION                 0x4000005fbL
#define SDB_CATALOGMGR_CREATEDOMAIN                        0x4000005fcL
#define SDB_CATALOGMGR_DROPDOMAIN                          0x4000005fdL
#define SDB_CATALOGMGR_ALTERDOMAIN                         0x4000005feL
#define SDB_CATALOGMGR__BUILDALTERGROUPS                   0x4000005ffL
#define SDB_CATALOGMGR__CHOOSEFGROUPOFCL                   0x400000600L
#define SDB_CATALOGMGR_AUTOHASHSPLIT                       0x400000601L
#define SDB_CATALOGMGR__COMBINEOPTIONS                     0x400000602L
#define SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ        0x400000603L
#define SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS             0x400000604L
#define SDB_CATMAINCT_HANDLEMSG                            0x400000605L
#define SDB_CATMAINCT_POSTMSG                              0x400000606L
#define SDB_CATMAINCT_INIT                                 0x400000607L
#define SDB_CATMAINCT__CREATESYSIDX                        0x400000608L
#define SDB_CATMAINCT__CREATESYSCOL                        0x400000609L
#define SDB_CATMAINCT__ENSUREMETADATA                      0x40000060aL
#define SDB_CATMAINCT_ACTIVE                               0x40000060bL
#define SDB_CATMAINCT_DEACTIVE                             0x40000060cL
#define SDB_CATMAINCT_BUILDMSGEVENT                        0x40000060dL
#define SDB_CATMAINCT_GETMOREMSG                           0x40000060eL
#define SDB_CATMAINCT_KILLCONTEXT                          0x40000060fL
#define SDB_CATMAINCT_QUERYMSG                             0x400000610L
#define SDB_CATMAINCT_QUERYREQUEST                         0x400000611L
#define SDB_CATMAINCT_AUTHCRT                              0x400000612L
#define SDB_CATMAINCT_AUTHENTICATE                         0x400000613L
#define SDB_CATMAINCT_AUTHDEL                              0x400000614L
#define SDB_CATMAINCT_CHECKROUTEID                         0x400000615L
#define SDB_CATGROUPNAMEVALIDATE                           0x400000616L
#define SDB_CATDOMAINOPTIONSEXTRACT                        0x400000617L
#define SDB_CATRESOLVECOLLECTIONNAME                       0x400000618L
#define SDB_CATQUERYANDGETMORE                             0x400000619L
#define SDB_CATGETONEOBJ                                   0x40000061aL
#define SDB_CATGETGROUPOBJ                                 0x40000061bL
#define SDB_CATGETGROUPOBJ1                                0x40000061cL
#define SDB_CATGETGROUPOBJ2                                0x40000061dL
#define SDB_CATGROUPCHECK                                  0x40000061eL
#define SDB_CATSERVICECHECK                                0x40000061fL
#define SDB_CATGROUPID2NAME                                0x400000620L
#define SDB_CATGROUPNAME2ID                                0x400000621L
#define SDB_CATGETDOMAINOBJ                                0x400000622L
#define SDB_CATDOMAINCHECK                                 0x400000623L
#define SDB_CATGETDOMAINGROUPS                             0x400000624L
#define SDB_CATGETDOMAINGROUPS1                            0x400000625L
#define SDB_CATADDGRP2DOMAIN                               0x400000626L
#define SDB_CATDELGRPFROMDOMAIN                            0x400000627L
#define SDB_CAATADDCL2CS                                   0x400000628L
#define SDB_CATDELCLFROMCS                                 0x400000629L
#define SDB_CATRESTORECS                                   0x40000062aL
#define SDB_CATGETCSGROUPS                                 0x40000062bL
#define SDB_CATCHECKSPACEEXIST                             0x40000062cL
#define SDB_CATREMOVECL                                    0x40000062dL
#define SDB_CATCHECKCOLLECTIONEXIST                        0x40000062eL
#define SDB_CATUPDATECATALOG                               0x40000062fL
#define SDB_CATADDTASK                                     0x400000630L
#define SDB_CATGETTASK                                     0x400000631L
#define SDB_CATGETTASKSTATUS                               0x400000632L
#define SDB_CATGETMAXTASKID                                0x400000633L
#define SDB_CATUPDATETASKSTATUS                            0x400000634L
#define SDB_CATREMOVETASK                                  0x400000635L
#define SDB_CATREMOVETASK1                                 0x400000636L
#define SDB_CATREMOVECLEX                                  0x400000637L
#define SDB_CATREMOVECSEX                                  0x400000638L
#define SDB_CATPRASEFUNC                                   0x400000639L
#define SDB_CATLINKCL                                      0x40000063aL
#define SDB_CATUNLINKCL                                    0x40000063bL
#define SDB_CATNODEMGR_INIT                                0x40000063cL
#define SDB_CATNODEMGR_ACTIVE                              0x40000063dL
#define SDB_CATNODEMGR_DEACTIVE                            0x40000063eL
#define SDB_CATNODEMGR_PROCESSMSG                          0x40000063fL
#define SDB_CATNODEMGR_PRIMARYCHANGE                       0x400000640L
#define SDB_CATNODEMGR_GRPREQ                              0x400000641L
#define SDB_CATNODEMGR_REGREQ                              0x400000642L
#define SDB_CATNODEMGR_PCREATEGRP                          0x400000643L
#define SDB_CATNODEMGR_CREATENODE                          0x400000644L
#define SDB_CATNODEMGR_UPDATENODE                          0x400000645L
#define SDB_CATNODEMGR_DELNODE                             0x400000646L
#define SDB_CATNODEMGR_PREMOVEGRP                          0x400000647L
#define SDB_CATNODEMGR_ACTIVEGRP                           0x400000648L
#define SDB_CATNODEMGR_READCATACONF                        0x400000649L
#define SDB_CATNODEMGR_PARSECATCONF                        0x40000064aL
#define SDB_CATNODEMGR_SAVEGRPINFO                         0x40000064bL
#define SDB_CATNODEMGR_GENGROUPINFO                        0x40000064cL
#define SDB_CATNODEMGR_GETNODEINFOBYCONF                   0x40000064dL
#define SDB_CATNODEMGR_PARSELINE                           0x40000064eL
#define SDB_CATNODEMGR_PARSEIDINFO                         0x40000064fL
#define SDB_CATNODEMGR_GETNODEINFO                         0x400000650L
#define SDB_CATNODEMGR_CREATEGRP                           0x400000651L
#define SDB_CATNODEMGR_REMOVEGRP                           0x400000652L
#define SDB_CATALOGCB_INIT                                 0x400000653L
#define SDB_CATALOGCB_INSERTGROUPID                        0x400000654L
#define SDB_CATALOGCB_REMOVEGROUPID                        0x400000655L
#define SDB_CATALOGCB_ACTIVEGROUP                          0x400000656L
#define SDB_CATALOGCB_INSERTNODEID                         0x400000657L
#define SDB_CATALOGCB_GETAGROUPRAND                        0x400000658L
#define SDB_CATALOGCB_ALLOCGROUPID                         0x400000659L
#define SDB_CATALOGCB_ALLOCCATANODEID                      0x40000065aL
#define SDB_CATALOGCB_ALLOCNODEID                          0x40000065bL
#define SDB_CATALOGCB_UPDATEROUTEID                        0x40000065cL
#endif
