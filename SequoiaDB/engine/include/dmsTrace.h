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
#define SDB__DMSROUNIT__INIT                               0x1000000000773L
#define SDB__DMSROUNIT_CLNUP                               0x1000000000774L
#define SDB__DMSROUNIT_OPEN                                0x1000000000775L
#define SDB__DMSROUNIT_IMPMME                              0x1000000000776L
#define SDB__DMSROUNIT_EXPMME                              0x1000000000777L
#define SDB__DMSROUNIT__ALCEXT                             0x1000000000778L
#define SDB__DMSROUNIT__FLSEXT                             0x1000000000779L
#define SDB__DMSROUNIT_FLUSH                               0x100000000077aL
#define SDB__DMSROUNIT_INSRCD                              0x100000000077bL
#define SDB__DMSROUNIT_GETNXTEXTSIZE                       0x100000000077cL
#define SDB__DMSROUNIT_EXPHEAD                             0x100000000077dL
#define SDB__DMSROUNIT_EXPEXT                              0x100000000077eL
#define SDB__DMSROUNIT_VLDHDBUFF                           0x100000000077fL
#define SDB__DMSSU                                         0x1000000000780L
#define SDB__DMSSU_DESC                                    0x1000000000781L
#define SDB__DMSSU_OPEN                                    0x1000000000782L
#define SDB__DMSSU_CLOSE                                   0x1000000000783L
#define SDB__DMSSU_REMOVE                                  0x1000000000784L
#define SDB__DMSSU__RESETCOLLECTION                        0x1000000000785L
#define SDB__DMSSU_LDEXTA                                  0x1000000000786L
#define SDB__DMSSU_LDEXT                                   0x1000000000787L
#define SDB__DMSSU_INSERTRECORD                            0x1000000000788L
#define SDB__DMSSU_UPDATERECORDS                           0x1000000000789L
#define SDB__DMSSU_DELETERECORDS                           0x100000000078aL
#define SDB__DMSSU_REBUILDINDEXES                          0x100000000078bL
#define SDB__DMSSU_CREATEINDEX                             0x100000000078cL
#define SDB__DMSSU_DROPINDEX                               0x100000000078dL
#define SDB__DMSSU_DROPINDEX1                              0x100000000078eL
#define SDB__DMSSU_COUNTCOLLECTION                         0x100000000078fL
#define SDB__DMSSU_GETCOLLECTIONFLAG                       0x1000000000790L
#define SDB__DMSSU_CHANGECOLLECTIONFLAG                    0x1000000000791L
#define SDB__DMSSU_GETCOLLECTIONATTRIBUTES                 0x1000000000792L
#define SDB__DMSSU_UPDATECOLLECTIONATTRIBUTES              0x1000000000793L
#define SDB__DMSSU_GETSEGEXTENTS                           0x1000000000794L
#define SDB__DMSSU_GETINDEXES                              0x1000000000795L
#define SDB__DMSSU_GETINDEX                                0x1000000000796L
#define SDB__DMSSU_DUMPINFO                                0x1000000000797L
#define SDB__DMSSU_DUMPINFO1                               0x1000000000798L
#define SDB__DMSSU_DUMPINFO2                               0x1000000000799L
#define SDB__DMSSU_TOTALSIZE                               0x100000000079aL
#define SDB__DMSSU_TOTALDATAPAGES                          0x100000000079bL
#define SDB__DMSSU_TOTALDATASIZE                           0x100000000079cL
#define SDB__DMSSU_TOTALFREEPAGES                          0x100000000079dL
#define SDB__DMSSU_GETSTATINFO                             0x100000000079eL
#define SDB__DMS_LOBDIRECTBUF__EXTENDBUF                   0x100000000079fL
#define SDB_DMSSTORAGELOBDATA_OPEN                         0x10000000007a0L
#define SDB_DMSSTORAGELOBDATA__REOPEN                      0x10000000007a1L
#define SDB_DMSSTORAGELOBDATA_CLOSE                        0x10000000007a2L
#define SDB_DMSSTORAGELOBDATA_WRITE                        0x10000000007a3L
#define SDB_DMSSTORAGELOBDATA_READ                         0x10000000007a4L
#define SDB_DMSSTORAGELOBDATA_READRAW                      0x10000000007a5L
#define SDB_DMSSTORAGELOBDATA_EXTEND                       0x10000000007a6L
#define SDB_DMSSTORAGELOBDATA__EXTEND                      0x10000000007a7L
#define SDB_DMSSTORAGELOBDATA_REMOVE                       0x10000000007a8L
#define SDB_DMSSTORAGELOBDATA__INITFILEHEADER              0x10000000007a9L
#define SDB_DMSSTORAGELOBDATA__VALIDATEFILE                0x10000000007aaL
#define SDB_DMSSTORAGELOBDATA__FETFILEHEADER               0x10000000007abL
#define SDB__DMSSTORAGELOADEXT__ALLOCEXTENT                0x10000000007acL
#define SDB__DMSSTORAGELOADEXT__IMPRTBLOCK                 0x10000000007adL
#define SDB__DMSSTORAGELOADEXT__LDDATA                     0x10000000007aeL
#define SDB__DMSSTORAGELOADEXT__ROLLEXTENT                 0x10000000007afL
#define SDB__DMSSTORAGELOB_OPEN                            0x10000000007b0L
#define SDB__DMSSTORAGELOB__DELAYOPEN                      0x10000000007b1L
#define SDB__DMSSTORAGELOB__OPENLOB                        0x10000000007b2L
#define SDB__DMSSTORAGELOB_REMOVESTORAGEFILES              0x10000000007b3L
#define SDB__DMSSTORAGELOB_GETLOBMETA                      0x10000000007b4L
#define SDB__DMSSTORAGELOB_WRITELOBMETA                    0x10000000007b5L
#define SDB__DMSSTORAGELOB_WRITE                           0x10000000007b6L
#define SDB__DMSSTORAGELOB_UPDATE                          0x10000000007b7L
#define SDB__DMSSTORAGELOB_READ                            0x10000000007b8L
#define SDB__DMSSTORAGELOB__ALLOCATEPAGE                   0x10000000007b9L
#define SDB__DMSSTORAGELOB__FILLPAGE                       0x10000000007baL
#define SDB__DMSSTORAGELOB_REMOVE                          0x10000000007bbL
#define SDB__DMSSTORAGELOB__FIND                           0x10000000007bcL
#define SDB__DMSSTORAGELOB__PUSH2BUCKET                    0x10000000007bdL
#define SDB__DMSSTORAGELOB__ONCREATE                       0x10000000007beL
#define SDB__DMSSTORAGELOB__ONMAPMETA                      0x10000000007bfL
#define SDB__DMSSTORAGELOB__EXTENDSEGMENTS                 0x10000000007c0L
#define SDB__DMSSTORAGELOB_READPAGE                        0x10000000007c1L
#define SDB__DMSSTORAGELOB__REMOVEPAGE                     0x10000000007c2L
#define SDB__DMSSTORAGELOB_TRUNCATE                        0x10000000007c3L
#define SDB__DMS_LOBDIRECTINBUF_GETALIGNEDTUPLE            0x10000000007c4L
#define SDB__DMS_LOBDIRECTINBUF_CP2USRBUF                  0x10000000007c5L
#define SDB__DMSSMS__RSTMAX                                0x10000000007c6L
#define SDB__DMSSMS_RSVPAGES                               0x10000000007c7L
#define SDB__DMSSMS_RLSPAGES                               0x10000000007c8L
#define SDB__DMSSMEMGR_INIT                                0x10000000007c9L
#define SDB__DMSSMEMGR_RSVPAGES                            0x10000000007caL
#define SDB__DMSSMEMGR_RLSPAGES                            0x10000000007cbL
#define SDB__DMSSMEMGR_DEPOSIT                             0x10000000007ccL
#define SDB__DMSTMPCB_INIT                                 0x10000000007cdL
#define SDB__DMSTMPCB_RELEASE                              0x10000000007ceL
#define SDB__DMSTMPCB_RESERVE                              0x10000000007cfL
#define SDB__SDB_DMSCB__LGCSCBNMMAP                        0x10000000007d0L
#define SDB__SDB_DMSCB__CSCBNMINST                         0x10000000007d1L
#define SDB__SDB_DMSCB__CSCBNMREMV                         0x10000000007d2L
#define SDB__SDB_DMSCB__CSCBNMREMVP1                       0x10000000007d3L
#define SDB__SDB_DMSCB__CSCBNMREMVP1CANCEL                 0x10000000007d4L
#define SDB__SDB_DMSCB__CSCBNMREMVP2                       0x10000000007d5L
#define SDB__SDB_DMSCB__CSCBNMMAPCLN                       0x10000000007d6L
#define SDB__SDB_DMSCB_ADDCS                               0x10000000007d7L
#define SDB__SDB_DMSCB_DELCS                               0x10000000007d8L
#define SDB__SDB_DMSCB_DROPCSP1                            0x10000000007d9L
#define SDB__SDB_DMSCB_DROPCSP1CANCEL                      0x10000000007daL
#define SDB__SDB_DMSCB_DROPCSP2                            0x10000000007dbL
#define SDB__SDB_DMSCB_DUMPINFO                            0x10000000007dcL
#define SDB__SDB_DMSCB_DUMPINFO2                           0x10000000007ddL
#define SDB__SDB_DMSCB_DUMPINFO3                           0x10000000007deL
#define SDB__SDB_DMSCB_DUMPINFO4                           0x10000000007dfL
#define SDB__SDB_DMSCB_DISPATCHPAGECLEANSU                 0x10000000007e0L
#define SDB__SDB_DMSCB_JOINPAGECLEANSU                     0x10000000007e1L
#define SDB__SDB_DMSCB__JOINPAGECLEANSU                    0x10000000007e2L
#define SDB_DMSCHKCSNM                                     0x10000000007e3L
#define SDB_DMSCHKFULLCLNM                                 0x10000000007e4L
#define SDB_DMSCHKCLNM                                     0x10000000007e5L
#define SDB_DMSCHKINXNM                                    0x10000000007e6L
#define SDB__MBFLAG2STRING                                 0x10000000007e7L
#define SDB__MBATTR2STRING                                 0x10000000007e8L
#define SDB__DMSMBCONTEXT                                  0x10000000007e9L
#define SDB__DMSMBCONTEXT_DESC                             0x10000000007eaL
#define SDB__DMSMBCONTEXT__RESET                           0x10000000007ebL
#define SDB__DMSMBCONTEXT_PAUSE                            0x10000000007ecL
#define SDB__DMSMBCONTEXT_RESUME                           0x10000000007edL
#define SDB__DMSSTORAGEDATA                                0x10000000007eeL
#define SDB__DMSSTORAGEDATA_DESC                           0x10000000007efL
#define SDB__DMSSTORAGEDATA_SYNCMEMTOMMAP                  0x10000000007f0L
#define SDB__DMSSTORAGEDATA__ONCREATE                      0x10000000007f1L
#define SDB__DMSSTORAGEDATA__ONMAPMETA                     0x10000000007f2L
#define SDB__DMSSTORAGEDATA__ONCLOSED                      0x10000000007f3L
#define SDB__DMSSTORAGEDATA__INITMME                       0x10000000007f4L
#define SDB__DMSSTORAGEDATA__LOGDPS                        0x10000000007f5L
#define SDB__DMSSTORAGEDATA__LOGDPS1                       0x10000000007f6L
#define SDB__DMSSTORAGEDATA__ALLOCATEEXTENT                0x10000000007f7L
#define SDB__DMSSTORAGEDATA__FREEEXTENT                    0x10000000007f8L
#define SDB__DMSSTORAGEDATA__RESERVEFROMDELETELIST         0x10000000007f9L
#define SDB__DMSSTORAGEDATA__TRUNCATECOLLECTION            0x10000000007faL
#define SDB__DMSSTORAGEDATA__TRUNCATECOLLECITONLOADS       0x10000000007fbL
#define SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD             0x10000000007fcL
#define SDB__DMSSTORAGEDATA__SAVEDELETEDRECORD1            0x10000000007fdL
#define SDB__DMSSTORAGEDATA__MAPEXTENT2DELLIST             0x10000000007feL
#define SDB__DMSSTORAGEDATA_ADDEXTENT2META                 0x10000000007ffL
#define SDB__DMSSTORAGEDATA_ADDCOLLECTION                  0x1000000000800L
#define SDB__DMSSTORAGEDATA_DROPCOLLECTION                 0x1000000000801L
#define SDB__DMSSTORAGEDATA_TRUNCATECOLLECTION             0x1000000000802L
#define SDB__DMSSTORAGEDATA_TRUNCATECOLLECTIONLOADS        0x1000000000803L
#define SDB__DMSSTORAGEDATA_RENAMECOLLECTION               0x1000000000804L
#define SDB__DMSSTORAGEDATA_FINDCOLLECTION                 0x1000000000805L
#define SDB__DMSSTORAGEDATA_INSERTRECORD                   0x1000000000806L
#define SDB__DMSSTORAGEDATA__EXTENTINSERTRECORD            0x1000000000807L
#define SDB__DMSSTORAGEDATA_DELETERECORD                   0x1000000000808L
#define SDB__DMSSTORAGEDATA__EXTENTREMOVERECORD            0x1000000000809L
#define SDB__DMSSTORAGEDATA_UPDATERECORD                   0x100000000080aL
#define SDB__DMSSTORAGEDATA__EXTENTUPDATERECORD            0x100000000080bL
#define SDB__DMSSTORAGEDATA_FETCH                          0x100000000080cL
#define SDB__DMS_LOBDIRECTOUTBUF_GETALIGNEDTUPLE           0x100000000080dL
#endif
