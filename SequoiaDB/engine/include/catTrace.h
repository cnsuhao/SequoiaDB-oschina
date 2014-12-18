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
#ifndef catTRACE_H__
#define catTRACE_H__
#define SDB_CATNODEMGR_INIT                                0x400000224L
#define SDB_CATNODEMGR_ACTIVE                              0x400000225L
#define SDB_CATNODEMGR_DEACTIVE                            0x400000226L
#define SDB_CATNODEMGR_PROCESSMSG                          0x400000227L
#define SDB_CATNODEMGR_PRIMARYCHANGE                       0x400000228L
#define SDB_CATNODEMGR_GRPREQ                              0x400000229L
#define SDB_CATNODEMGR_REGREQ                              0x40000022aL
#define SDB_CATNODEMGR_PCREATEGRP                          0x40000022bL
#define SDB_CATNODEMGR_CREATENODE                          0x40000022cL
#define SDB_CATNODEMGR_UPDATENODE                          0x40000022dL
#define SDB_CATNODEMGR_DELNODE                             0x40000022eL
#define SDB_CATNODEMGR_PREMOVEGRP                          0x40000022fL
#define SDB_CATNODEMGR_ACTIVEGRP                           0x400000230L
#define SDB_CATNODEMGR_READCATACONF                        0x400000231L
#define SDB_CATNODEMGR_PARSECATCONF                        0x400000232L
#define SDB_CATNODEMGR_SAVEGRPINFO                         0x400000233L
#define SDB_CATNODEMGR_GENGROUPINFO                        0x400000234L
#define SDB_CATNODEMGR_GETNODEINFOBYCONF                   0x400000235L
#define SDB_CATNODEMGR_PARSELINE                           0x400000236L
#define SDB_CATNODEMGR_PARSEIDINFO                         0x400000237L
#define SDB_CATNODEMGR_GETNODEINFO                         0x400000238L
#define SDB_CATNODEMGR_CREATEGRP                           0x400000239L
#define SDB_CATNODEMGR_REMOVEGRP                           0x40000023aL
#define SDB_CATMAINCT_HANDLEMSG                            0x40000023bL
#define SDB_CATMAINCT_POSTMSG                              0x40000023cL
#define SDB_CATMAINCT_INIT                                 0x40000023dL
#define SDB_CATMAINCT__CREATESYSIDX                        0x40000023eL
#define SDB_CATMAINCT__CREATESYSCOL                        0x40000023fL
#define SDB_CATMAINCT__ENSUREMETADATA                      0x400000240L
#define SDB_CATMAINCT_ACTIVE                               0x400000241L
#define SDB_CATMAINCT_DEACTIVE                             0x400000242L
#define SDB_CATMAINCT_BUILDMSGEVENT                        0x400000243L
#define SDB_CATMAINCT_GETMOREMSG                           0x400000244L
#define SDB_CATMAINCT_KILLCONTEXT                          0x400000245L
#define SDB_CATMAINCT_QUERYMSG                             0x400000246L
#define SDB_CATMAINCT_QUERYREQUEST                         0x400000247L
#define SDB_CATMAINCT_AUTHCRT                              0x400000248L
#define SDB_CATMAINCT_AUTHENTICATE                         0x400000249L
#define SDB_CATMAINCT_AUTHDEL                              0x40000024aL
#define SDB_CATMAINCT_CHECKROUTEID                         0x40000024bL
#define SDB_CATALOGCB_INIT                                 0x40000024cL
#define SDB_CATALOGCB_INSERTGROUPID                        0x40000024dL
#define SDB_CATALOGCB_REMOVEGROUPID                        0x40000024eL
#define SDB_CATALOGCB_ACTIVEGROUP                          0x40000024fL
#define SDB_CATALOGCB_INSERTNODEID                         0x400000250L
#define SDB_CATALOGCB_GETAGROUPRAND                        0x400000251L
#define SDB_CATALOGCB_ALLOCGROUPID                         0x400000252L
#define SDB_CATALOGCB_ALLOCCATANODEID                      0x400000253L
#define SDB_CATALOGCB_ALLOCNODEID                          0x400000254L
#define SDB_CATALOGCB_UPDATEROUTEID                        0x400000255L
#define SDB_CATGROUPNAMEVALIDATE                           0x400000256L
#define SDB_CATDOMAINOPTIONSEXTRACT                        0x400000257L
#define SDB_CATRESOLVECOLLECTIONNAME                       0x400000258L
#define SDB_CATQUERYANDGETMORE                             0x400000259L
#define SDB_CATGETONEOBJ                                   0x40000025aL
#define SDB_CATGETGROUPOBJ                                 0x40000025bL
#define SDB_CATGETGROUPOBJ1                                0x40000025cL
#define SDB_CATGETGROUPOBJ2                                0x40000025dL
#define SDB_CATGROUPCHECK                                  0x40000025eL
#define SDB_CATSERVICECHECK                                0x40000025fL
#define SDB_CATGROUPID2NAME                                0x400000260L
#define SDB_CATGROUPNAME2ID                                0x400000261L
#define SDB_CATGETDOMAINOBJ                                0x400000262L
#define SDB_CATDOMAINCHECK                                 0x400000263L
#define SDB_CATGETDOMAINGROUPS                             0x400000264L
#define SDB_CATGETDOMAINGROUPS1                            0x400000265L
#define SDB_CAATADDCL2CS                                   0x400000266L
#define SDB_CATDELCLFROMCS                                 0x400000267L
#define SDB_CATRESTORECS                                   0x400000268L
#define SDB_CATGETCSGROUPS                                 0x400000269L
#define SDB_CATCHECKSPACEEXIST                             0x40000026aL
#define SDB_CATREMOVECL                                    0x40000026bL
#define SDB_CATCHECKCOLLECTIONEXIST                        0x40000026cL
#define SDB_CATUPDATECATALOG                               0x40000026dL
#define SDB_CATADDTASK                                     0x40000026eL
#define SDB_CATGETTASK                                     0x40000026fL
#define SDB_CATGETTASKSTATUS                               0x400000270L
#define SDB_CATGETMAXTASKID                                0x400000271L
#define SDB_CATUPDATETASKSTATUS                            0x400000272L
#define SDB_CATREMOVETASK                                  0x400000273L
#define SDB_CATREMOVETASK1                                 0x400000274L
#define SDB_CATREMOVECLEX                                  0x400000275L
#define SDB_CATREMOVECSEX                                  0x400000276L
#define SDB_CATPRASEFUNC                                   0x400000277L
#define SDB_CATLINKCL                                      0x400000278L
#define SDB_CATUNLINKCL                                    0x400000279L
#define SDB_CATALOGMGR_DROPCS                              0x40000027aL
#define SDB_CATALOGMGR_CRT_PROCEDURES                      0x40000027bL
#define SDB_CATALOGMGR_RM_PROCEDURES                       0x40000027cL
#define SDB_CATALOGMGR_QUERYSPACEINFO                      0x40000027dL
#define SDB_CATALOGMGR_QUERYCATALOG                        0x40000027eL
#define SDB_CATALOGMGR_DROPCOLLECTION                      0x40000027fL
#define SDB_CATALOGMGR_QUERYTASK                           0x400000280L
#define SDB_CATALOGMGR_ALTERCOLLECTION                     0x400000281L
#define SDB_CATALOGMGR_CREATECS                            0x400000282L
#define SDB_CATALOGMGR_CREATECL                            0x400000283L
#define SDB_CATALOGMGR_CMDSPLIT                            0x400000284L
#define SDB_CATALOGMGR__CHECKCSOBJ                         0x400000285L
#define SDB_CATALOGMGR__CHECKANDBUILDCATARECORD            0x400000286L
#define SDB_CATALOGMGR__ASSIGNGROUP                        0x400000287L
#define SDB_CATALOGMGR__CHECKGROUPINDOMAIN                 0x400000288L
#define SDB_CATALOGMGR__CREATECS                           0x400000289L
#define SDB_CATALOGMGR_CREATECOLLECTION                    0x40000028aL
#define SDB_CATALOGMGR_BUILDCATALOGRECORD                  0x40000028bL
#define SDB_CATALOGMGR_BUILDINITBOUND                      0x40000028cL
#define SDB_CATALOGMGR_PROCESSMSG                          0x40000028dL
#define SDB_CATALOGMGR_PROCESSCOMMANDMSG                   0x40000028eL
#define SDB_CATALOGMGR__BUILDHASHBOUND                     0x40000028fL
#define SDB_CATALOGMGR_CMDLINKCOLLECTION                   0x400000290L
#define SDB_CATALOGMGR_CMDUNLINKCOLLECTION                 0x400000291L
#define SDB_CATALOGMGR_CREATEDOMAIN                        0x400000292L
#define SDB_CATALOGMGR_DROPDOMAIN                          0x400000293L
#define SDB_CATALOGMGR_ALTERDOMAIN                         0x400000294L
#define SDB_CATALOGMGR__BUILDALTERGROUPS                   0x400000295L
#define SDB_CATALOGMGR__CHOOSEFGROUPOFCL                   0x400000296L
#define SDB_CATALOGMGR__GETGPFROMCS                        0x400000297L
#define SDB_CATALOGMGR_AUTOHASHSPLIT                       0x400000298L
#define SDB_CATALOGMGR__COMBINEOPTIONS                     0x400000299L
#define SDB_CATALOGMGR__BUILDALTEROBJWITHMETAANDOBJ        0x40000029aL
#define SDB_CATALOGMGR__GETGROUPSOFCOLLECTIONS             0x40000029bL
#endif
