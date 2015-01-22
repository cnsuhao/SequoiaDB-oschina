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
#ifndef dmsTRACE_H__
#define dmsTRACE_H__
#define SDB__DMS_LOBDIRECTOUTBUF_GETALIGNEDTUPLE           0x1000000000809L
#define SDB__DMSSTORAGELOADEXT__ALLOCEXTENT                0x100000000080aL
#define SDB__DMSSTORAGELOADEXT__IMPRTBLOCK                 0x100000000080bL
#define SDB__DMSSTORAGELOADEXT__LDDATA                     0x100000000080cL
#define SDB__DMSSTORAGELOADEXT__ROLLEXTENT                 0x100000000080dL
#define SDB__DMSROUNIT__INIT                               0x100000000080eL
#define SDB__DMSROUNIT_CLNUP                               0x100000000080fL
#define SDB__DMSROUNIT_OPEN                                0x1000000000810L
#define SDB__DMSROUNIT_IMPMME                              0x1000000000811L
#define SDB__DMSROUNIT_EXPMME                              0x1000000000812L
#define SDB__DMSROUNIT__ALCEXT                             0x1000000000813L
#define SDB__DMSROUNIT__FLSEXT                             0x1000000000814L
#define SDB__DMSROUNIT_FLUSH                               0x1000000000815L
#define SDB__DMSROUNIT_INSRCD                              0x1000000000816L
#define SDB__DMSROUNIT_GETNXTEXTSIZE                       0x1000000000817L
#define SDB__DMSROUNIT_EXPHEAD                             0x1000000000818L
#define SDB__DMSROUNIT_EXPEXT                              0x1000000000819L
#define SDB__DMSROUNIT_VLDHDBUFF                           0x100000000081aL
#define SDB_DMSSTORAGELOBDATA_OPEN                         0x100000000081bL
#define SDB_DMSSTORAGELOBDATA__REOPEN                      0x100000000081cL
#define SDB_DMSSTORAGELOBDATA_CLOSE                        0x100000000081dL
#define SDB_DMSSTORAGELOBDATA_WRITE                        0x100000000081eL
#define SDB_DMSSTORAGELOBDATA_READ                         0x100000000081fL
#define SDB_DMSSTORAGELOBDATA_READRAW                      0x1000000000820L
#define SDB_DMSSTORAGELOBDATA_EXTEND                       0x1000000000821L
#define SDB_DMSSTORAGELOBDATA__EXTEND                      0x1000000000822L
#define SDB_DMSSTORAGELOBDATA_REMOVE                       0x1000000000823L
#define SDB_DMSSTORAGELOBDATA__INITFILEHEADER              0x1000000000824L
#define SDB_DMSSTORAGELOBDATA__VALIDATEFILE                0x1000000000825L
#define SDB_DMSSTORAGELOBDATA__FETFILEHEADER               0x1000000000826L
#define SDB_DMSCHKCSNM                                     0x1000000000827L
#define SDB_DMSCHKFULLCLNM                                 0x1000000000828L
#define SDB_DMSCHKCLNM                                     0x1000000000829L
#define SDB_DMSCHKINXNM                                    0x100000000082aL
#define SDB__SDB_DMSCB__LGCSCBNMMAP                        0x100000000082bL
#define SDB__SDB_DMSCB__CSCBNMINST                         0x100000000082cL
#define SDB__SDB_DMSCB__CSCBNMREMV                         0x100000000082dL
#define SDB__SDB_DMSCB__CSCBNMREMVP1                       0x100000000082eL
#define SDB__SDB_DMSCB__CSCBNMREMVP1CANCEL                 0x100000000082fL
#define SDB__SDB_DMSCB__CSCBNMREMVP2                       0x1000000000830L
#define SDB__SDB_DMSCB__CSCBNMMAPCLN                       0x1000000000831L
#define SDB__SDB_DMSCB_ADDCS                               0x1000000000832L
#define SDB__SDB_DMSCB_DELCS                               0x1000000000833L
#define SDB__SDB_DMSCB_DROPCSP1                            0x1000000000834L
#define SDB__SDB_DMSCB_DROPCSP1CANCEL                      0x1000000000835L
#define SDB__SDB_DMSCB_DROPCSP2                            0x1000000000836L
#define SDB__SDB_DMSCB_DUMPINFO                            0x1000000000837L
#define SDB__SDB_DMSCB_DUMPINFO2                           0x1000000000838L
#define SDB__SDB_DMSCB_DUMPINFO3                           0x1000000000839L
#define SDB__SDB_DMSCB_DUMPINFO4                           0x100000000083aL
#define SDB__SDB_DMSCB_DISPATCHPAGECLEANSU                 0x100000000083bL
#define SDB__SDB_DMSCB_JOINPAGECLEANSU                     0x100000000083cL
#define SDB__SDB_DMSCB__JOINPAGECLEANSU                    0x100000000083dL
#define SDB__DMSSU                                         0x100000000083eL
#define SDB__DMSSU_DESC                                    0x100000000083fL
#define SDB__DMSSU_OPEN                                    0x1000000000840L
#define SDB__DMSSU_CLOSE                                   0x1000000000841L
#define SDB__DMSSU_REMOVE                                  0x1000000000842L
#define SDB__DMSSU__RESETCOLLECTION                        0x1000000000843L
#define SDB__DMSSU_LDEXTA                                  0x1000000000844L
#define SDB__DMSSU_LDEXT                                   0x1000000000845L
#define SDB__DMSSU_INSERTRECORD                            0x1000000000846L
#define SDB__DMSSU_UPDATERECORDS                           0x1000000000847L
#define SDB__DMSSU_DELETERECORDS                           0x1000000000848L
#define SDB__DMSSU_REBUILDINDEXES                          0x1000000000849L
#define SDB__DMSSU_CREATEINDEX                             0x100000000084aL
#define SDB__DMSSU_DROPINDEX                               0x100000000084bL
#define SDB__DMSSU_DROPINDEX1                              0x100000000084cL
#define SDB__DMSSU_COUNTCOLLECTION                         0x100000000084dL
#define SDB__DMSSU_GETCOLLECTIONFLAG                       0x100000000084eL
#define SDB__DMSSU_CHANGECOLLECTIONFLAG                    0x100000000084fL
#define SDB__DMSSU_GETCOLLECTIONATTRIBUTES                 0x1000000000850L
#define SDB__DMSSU_UPDATECOLLECTIONATTRIBUTES              0x1000000000851L
#define SDB__DMSSU_GETSEGEXTENTS                           0x1000000000852L
#define SDB__DMSSU_GETINDEXES                              0x1000000000853L
#define SDB__DMSSU_GETINDEX                                0x1000000000854L
#define SDB__DMSSU_DUMPINFO                                0x1000000000855L
#define SDB__DMSSU_DUMPINFO1                               0x1000000000856L
#define SDB__DMSSU_DUMPINFO2                               0x1000000000857L
#define SDB__DMSSU_TOTALSIZE                               0x1000000000858L
#define SDB__DMSSU_TOTALDATAPAGES                          0x1000000000859L
#define SDB__DMSSU_TOTALDATASIZE                           0x100000000085aL
#define SDB__DMSSU_TOTALFREEPAGES                          0x100000000085bL
#define SDB__DMSSU_GETSTATINFO                             0x100000000085cL
#define SDB__DMS_LOBDIRECTBUF__EXTENDBUF                   0x100000000085dL
#define SDB__DMSTMPCB_INIT                                 0x100000000085eL
#define SDB__DMSTMPCB_RELEASE                              0x100000000085fL
#define SDB__DMSTMPCB_RESERVE                              0x1000000000860L
#define SDB__DMSSTORAGELOB_OPEN                            0x1000000000861L
#define SDB__DMSSTORAGELOB__DELAYOPEN                      0x1000000000862L
#define SDB__DMSSTORAGELOB__OPENLOB                        0x1000000000863L
#define SDB__DMSSTORAGELOB_REMOVESTORAGEFILES              0x1000000000864L
#define SDB__DMSSTORAGELOB_GETLOBMETA                      0x1000000000865L
#define SDB__DMSSTORAGELOB_WRITELOBMETA                    0x1000000000866L
#define SDB__DMSSTORAGELOB_WRITE                           0x1000000000867L
#define SDB__DMSSTORAGELOB_UPDATE                          0x1000000000868L
#define SDB__DMSSTORAGELOB_READ                            0x1000000000869L
#define SDB__DMSSTORAGELOB__ALLOCATEPAGE                   0x100000000086aL
#define SDB__DMSSTORAGELOB__FILLPAGE                       0x100000000086bL
#define SDB__DMSSTORAGELOB_REMOVE                          0x100000000086cL
#define SDB__DMSSTORAGELOB__FIND                           0x100000000086dL
#define SDB__DMSSTORAGELOB__PUSH2BUCKET                    0x100000000086eL
#define SDB__DMSSTORAGELOB__ONCREATE                       0x100000000086fL
#define SDB__DMSSTORAGELOB__ONMAPMETA                      0x1000000000870L
#define SDB__DMSSTORAGELOB__EXTENDSEGMENTS                 0x1000000000871L
#define SDB__DMSSTORAGELOB_READPAGE                        0x1000000000872L
#define SDB__DMSSTORAGELOB__REMOVEPAGE                     0x1000000000873L
#define SDB__DMSSTORAGELOB_TRUNCATE                        0x1000000000874L
#define SDB__MBFLAG2STRING                                 0x1000000000875L
#define SDB__MBATTR2STRING                                 0x1000000000876L
#define SDB__DMSMBCONTEXT                                  0x1000000000877L
#define SDB__DMSMBCONTEXT_DESC                             0x1000000000878L
#define SDB__DMSMBCONTEXT__RESET                           0x1000000000879L
#define SDB__DMSMBCONTEXT_PAUSE                            0x100000000087aL
#define SDB__DMSMBCONTEXT_RESUME                           0x100000000087bL
#define SDB__DMSSTORAGEDATA                                0x100000000087cL
#define SDB__DMSSTORAGEDATA_DESC                           0x100000000087dL
#define SDB__DMSSTORAGEDATA_SYNCMEMTOMMAP                  0x100000000087eL
#define SDB__DMSSTORAGEDATA__ONCREATE                      0x100000000087fL
#define SDB__DMSSTORAGEDATA__ONMAPMETA                     0x1000000000880L
#define SDB__DMSSTORAGEDATA__ONCLOSED                      0x1000000000881L
#define SDB__DMSSTORAGEDATA__INITMME                       0x1000000000882L
#define SDB__DMSSTORAGEDATA__LOGDPS                        0x1000000000883L
#define SDB__DMSSTORAGEDATA__LOGDPS1                       0x1000000000884L
#define SDB__DMSSTORAGEDATA__ALLOCATEEXTENT                0x1000000000885L
#define SDB__DMSSTORAGEDATA__FREEEXTENT                    0x1000000000886L
#define SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST         0x1000000000887L
#define SDB__DMSSTORAGEDATA__TRUNCATECOLLECTION            0x1000000000888L
#define SDB__DMSSTORAGEDATA__TRUNCATECOLLECITONLOADS       0x1000000000889L
#define SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD             0x100000000088aL
#define SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1            0x100000000088bL
#define SDB__DMSSTORAGEDATA__MAPEXTENT2DELLIST             0x100000000088cL
#define SDB__DMSSTORAGEDATA_ADDEXTENT2META                 0x100000000088dL
#define SDB__DMSSTORAGEDATA_ADDCOLLECTION                  0x100000000088eL
#define SDB__DMSSTORAGEDATA_DROPCOLLECTION                 0x100000000088fL
#define SDB__DMSSTORAGEDATA_TRUNCATECOLLECTION             0x1000000000890L
#define SDB__DMSSTORAGEDATA_TRUNCATECOLLECTIONLOADS        0x1000000000891L
#define SDB__DMSSTORAGEDATA_RENAMECOLLECTION               0x1000000000892L
#define SDB__DMSSTORAGEDATA_FINDCOLLECTION                 0x1000000000893L
#define SDB__DMSSTORAGEDATA_INSERTRECORD                   0x1000000000894L
#define SDB__DMSSTORAGEDATA__EXTENTINSERTRECORD            0x1000000000895L
#define SDB__DMSSTORAGEDATA_DELETERECORD                   0x1000000000896L
#define SDB__DMSSTORAGEDATA__EXTENTREMOVERECORD            0x1000000000897L
#define SDB__DMSSTORAGEDATA_UPDATERECORD                   0x1000000000898L
#define SDB__DMSSTORAGEDATA__EXTENTUPDATERECORD            0x1000000000899L
#define SDB__DMSSTORAGEDATA_FETCH                          0x100000000089aL
#define SDB__DMSSMS__RSTMAX                                0x100000000089bL
#define SDB__DMSSMS_RSVPAGES                               0x100000000089cL
#define SDB__DMSSMS_RLSPAGES                               0x100000000089dL
#define SDB__DMSSMEMGR_INIT                                0x100000000089eL
#define SDB__DMSSMEMGR_RSVPAGES                            0x100000000089fL
#define SDB__DMSSMEMGR_RLSPAGES                            0x10000000008a0L
#define SDB__DMSSMEMGR_DEPOSIT                             0x10000000008a1L
#define SDB__DMS_LOBDIRECTINBUF_GETALIGNEDTUPLE            0x10000000008a2L
#define SDB__DMS_LOBDIRECTINBUF_CP2USRBUF                  0x10000000008a3L
#endif
