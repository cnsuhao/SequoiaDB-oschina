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
#ifndef qgmTRACE_H__
#define qgmTRACE_H__
#define SDB__QGMPLNLJOIN__INIT                             0x8000000000003adL
#define SDB__QGMPLNLJOIN__EXEC                             0x8000000000003aeL
#define SDB__QGMPLNLJOIN__FETCHNEXT                        0x8000000000003afL
#define SDB__QGMPLNLJOIN__MODIFYINNERCONDITION             0x8000000000003b0L
#define SDB__QGMMATCHER_MATCH                              0x8000000000003b1L
#define SDB__QGMMATCHER__MATCH                             0x8000000000003b2L
#define SDB__QGMEXTENDPLAN_EXTEND                          0x8000000000003b3L
#define SDB__QGMEXTENDPLAN_INSERTPLAN                      0x8000000000003b4L
#define SDB__QGMPLDELETE__EXEC                             0x8000000000003b5L
#define SDB__QGMOPSTREAM_FIND                              0x8000000000003b6L
#define SDB__QGMOPRUNIT_ADDOPFIELD                         0x8000000000003b7L
#define SDB__QGMOPTITREENODE_ADDCHILD                      0x8000000000003b8L
#define SDB__QGMOPTITREENODE__ONPUSHOPRUNIT                0x8000000000003b9L
#define SDB__QGMOPTITREENODE_PUSHOPRUNIT                   0x8000000000003baL
#define SDB__QGMOPTITREENODE_RMOPRUNIT                     0x8000000000003bbL
#define SDB__QGMOPTITREENODE_UPCHANGE                      0x8000000000003bcL
#define SDB__QGMOPTITREENODE_OUTPUTSORT                    0x8000000000003bdL
#define SDB__QGMOPTITREENODE_EXTEND                        0x8000000000003beL
#define SDB__QGMOPTTREE__PREPARE                           0x8000000000003bfL
#define SDB__QGMOPTTREE_INSERTBETWEEN                      0x8000000000003c0L
#define SDB__QGMFETCHOUT_ELEMENT                           0x8000000000003c1L
#define SDB__QGMFETCHOUT_ELEMENTS                          0x8000000000003c2L
#define SDB__QGMEXTENDSELECTPLAN__EXTEND                   0x8000000000003c3L
#define SDB__QGMCONDITIONNODEHELPER_MERGE                  0x8000000000003c4L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE               0x8000000000003c5L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE2              0x8000000000003c6L
#define SDB__QGMCONDITIONNODEHELPER__CRTBSON               0x8000000000003c7L
#define SDB__QGMCONDITIONNODEHELPER__TOBSON                0x8000000000003c8L
#define SDB__QGMCONDITIONNODEHELPER__GETALLATTR            0x8000000000003c9L
#define SDB__QGMPLMTHMATCHERFILTER__FETCHNEXT              0x8000000000003caL
#define SDB__QGMPTRTABLE_GETFIELD                          0x8000000000003cbL
#define SDB__QGMPTRTABLE_GETOWNFIELD                       0x8000000000003ccL
#define SDB__QGMPTRTABLE_GETATTR                           0x8000000000003cdL
#define SDB__QGMPLFILTER__FETCHNEXT                        0x8000000000003ceL
#define SDB__QGMSELECTOR_SELECT                            0x8000000000003cfL
#define SDB__QGMSELECTOR_SELECT2                           0x8000000000003d0L
#define SDB__QGMPLUPDATE__EXEC                             0x8000000000003d1L
#define SDB__QGMHASHTBL_PUSH                               0x8000000000003d2L
#define SDB__QGMHASHTBL_FIND                               0x8000000000003d3L
#define SDB__QGMHASHTBL_GETMORE                            0x8000000000003d4L
#define SDB__QGMOPTISORT_INIT                              0x8000000000003d5L
#define SDB__QGMOPTISORT__PUSHOPRUNIT                      0x8000000000003d6L
#define SDB__QGMOPTISORT__RMOPRUNIT                        0x8000000000003d7L
#define SDB__QGMOPTISORT_APPEND                            0x8000000000003d8L
#define SDB__QGMPLSCAN__EXEC                               0x8000000000003d9L
#define SDB__QGMPLSCAN__EXECONDATA                         0x8000000000003daL
#define SDB__QGMPLSCAN__EXECONCOORD                        0x8000000000003dbL
#define SDB__QGMPLSCAN__FETCHNEXT                          0x8000000000003dcL
#define SDB__QGMPLSCAN__FETCH                              0x8000000000003ddL
#define SDB__QGMPLSPLITBY__FETCHNEXT                       0x8000000000003deL
#define SDB__QGMPLCOMMAND__EXEC                            0x8000000000003dfL
#define SDB__QGMPLCOMMAND_EXECONCOORD                      0x8000000000003e0L
#define SDB__QGMPLCOMMAND__EXECONDATA                      0x8000000000003e1L
#define SDB__QGMPLCOMMAND__FETCHNEXT                       0x8000000000003e2L
#define SDB__QGMBUILDER_BUILDORDERBY                       0x8000000000003e3L
#define SDB__QGMBUILDER_BUILD1                             0x8000000000003e4L
#define SDB__QGMBUILDER_BUILD2                             0x8000000000003e5L
#define SDB__QGMBUILDER__BUILDPHYNODE                      0x8000000000003e6L
#define SDB__QGMBUILDER__ADDPHYCOMMAND                     0x8000000000003e7L
#define SDB__QGMBUILDER__ADDPHYAGGR                        0x8000000000003e8L
#define SDB__QGMBUILDER__CRTPHYSORT                        0x8000000000003e9L
#define SDB__QGMBUILDER__ADDPHYSCAN                        0x8000000000003eaL
#define SDB__QGMBUILDER__ADDMTHMATHERSCAN                  0x8000000000003ebL
#define SDB__QGMBUILDER__CRTPHYJOIN                        0x8000000000003ecL
#define SDB__QGMBUILDER__CRTPHYFILTER                      0x8000000000003edL
#define SDB__QGMBUILDER__CRTMTHMATHERFILTER                0x8000000000003eeL
#define SDB__QGMBUILDER__BUILD1                            0x8000000000003efL
#define SDB__QGMBUILDER__BUILDUPDATE                       0x8000000000003f0L
#define SDB__QGMBUILDER__ADDSET                            0x8000000000003f1L
#define SDB__QGMBUILDER__BUILDDELETE                       0x8000000000003f2L
#define SDB__QGMBUILDER__BUILDDROPCL                       0x8000000000003f3L
#define SDB__QGMBUILDER__BUILDDROPINX                      0x8000000000003f4L
#define SDB__QGMBUILDER__BUILDCRTINX                       0x8000000000003f5L
#define SDB__QGMBUILDER__BUILDINXCOLUMNS                   0x8000000000003f6L
#define SDB__QGMBUILDER__BUILDCRTCL                        0x8000000000003f7L
#define SDB__QGMBUILDER__BUILDCRTCS                        0x8000000000003f8L
#define SDB__QGMBUILDER__BUILDDROPCS                       0x8000000000003f9L
#define SDB__QGMBUILDER__BUILDSELECT                       0x8000000000003faL
#define SDB__QGMBUILDER__BUILDINSERT                       0x8000000000003fbL
#define SDB__QGMBUILDER__ADDSELECTOR                       0x8000000000003fcL
#define SDB__QGMBUILDER__ADDFROM                           0x8000000000003fdL
#define SDB__QGMBUILDER__BUILDJOIN                         0x8000000000003feL
#define SDB__QGMBUILDER__BUILDINCONDITION                  0x8000000000003ffL
#define SDB__QGMBUILDER__BUILDCONDITION                    0x800000000000400L
#define SDB__QGMBUILDER__ADDSPLITBY                        0x800000000000401L
#define SDB__QGMBUILDER__ADDGROUPBY                        0x800000000000402L
#define SDB__QGMBUILDER__ADDORDERBY                        0x800000000000403L
#define SDB__QGBUILDER__ADDLIMIT                           0x800000000000404L
#define SDB__QGMBUILDER__ADDSKIP                           0x800000000000405L
#define SDB__QGMBUILDER__ADDCOLUMNS                        0x800000000000406L
#define SDB__QGMBUILDER__ADDVALUES                         0x800000000000407L
#define SDB__QGMBUILDER__ADDHINT                           0x800000000000408L
#define SDB__QGMPLAN_EXECUTE                               0x800000000000409L
#define SDB__QGMPLAN_FETCHNEXT                             0x80000000000040aL
#define SDB__QGMPLMTHMATCHERSCAN__EXEC                     0x80000000000040bL
#define SDB__QGMPLINSERT__MERGEOBJ                         0x80000000000040cL
#define SDB__QGMPLINSERT__NEXTRECORD                       0x80000000000040dL
#define SDB__QGMPLINSERT__EXEC                             0x80000000000040eL
#define SDB__QGMPLSORT__EXEC                               0x80000000000040fL
#define SDB__QGMPLSORT__FETCHNEXT                          0x800000000000410L
#define SDB__QGMOPTISELECT_INIT                            0x800000000000411L
#define SDB__QGMOPTISELECT_DONE                            0x800000000000412L
#define SDB__QGMOPTISELECT_OUTPUTSTREAM                    0x800000000000413L
#define SDB__QGMOPTISELECT__PUSHOPRUNIT                    0x800000000000414L
#define SDB__QGMOPTISELECT__RMOPRUNIT                      0x800000000000415L
#define SDB__QGMOPTISELECT__EXTEND                         0x800000000000416L
#define SDB__QGMOPTISELECT__VALIDATEANDCRTPLAN             0x800000000000417L
#define SDB__QGMOPTISELECT__PARAMEXISTINSELECOTR           0x800000000000418L
#define SDB__QGMPARAMTABLE_ADDCONST                        0x800000000000419L
#define SDB__QGMPARAMTABLE_ADDCONST2                       0x80000000000041aL
#define SDB__QGMPARATABLE_ADDVAR                           0x80000000000041bL
#define SDB__QGMPARAMTABLE_SETVAR                          0x80000000000041cL
#define SDB__QGMOPTINLJOIN__MAKECONDVAR                    0x80000000000041dL
#define SDB__QGMOPTINLJOIN_MAKECONDITION                   0x80000000000041eL
#define SDB__QGMOPTINLJOIN_INIT                            0x80000000000041fL
#define SDB__QGMOPTINLJOIN__CRTJOINUNIT                    0x800000000000420L
#define SDB__QGMOPTINLJOIN_OUTPUTSTREAM                    0x800000000000421L
#define SDB__QGMOPTINLJOIN__PUSHOPRUNIT                    0x800000000000422L
#define SDB__QGMOPTINLJOIN__UPDATECHANGE                   0x800000000000423L
#define SDB__QGMOPTINLJOIN_HANDLEHINTS                     0x800000000000424L
#define SDB__QGMOPTINLJOIN__VALIDATE                       0x800000000000425L
#define SDB__QGMPLHASHJOIN_INIT                            0x800000000000426L
#define SDB__QGMPLHASHJOIN__EXEC                           0x800000000000427L
#define SDB__QGMPLHASHJOIN__FETCHNEXT                      0x800000000000428L
#define SDB__QGMPLHASHJOIN__BUILDHASNTBL                   0x800000000000429L
#define SDB__QGMUTILFIRSTDOT                               0x80000000000042aL
#define SDB__QGMFINDFIELDFROMFUNC                          0x80000000000042bL
#define SDB__QGMISFROMONE                                  0x80000000000042cL
#define SDB__QGMISSAMEFROM                                 0x80000000000042dL
#define SDB__QGMMERGE                                      0x80000000000042eL
#define SDB__QGMREPLACEFIELDRELE                           0x80000000000042fL
#define SDB__QGMREPLACEATTRRELE                            0x800000000000430L
#define SDB__QGMREPLACEATTRRELE2                           0x800000000000431L
#define SDB__QGMREPLACEATTRRELE3                           0x800000000000432L
#define SDB__QDMDOWNFIELDSBYFIELDALIAS                     0x800000000000433L
#define SDB__QGMDOWNATTRSBYFIELDALIAS                      0x800000000000434L
#define SDB__QGMDOWNATTRSBYFIELDALIAS2                     0x800000000000435L
#define SDB__QGMDOWNAATTRBYFIELDALIAS                      0x800000000000436L
#define SDB__QGMDOWNAGGRSBYFIELDALIAS                      0x800000000000437L
#define SDB__QGMUPFIELDSBYFIELDALIAS                       0x800000000000438L
#define SDB__QGMUPATTRSBYFIELDALIAS                        0x800000000000439L
#define SDB__QGMUPATTRSBYFIELDALIAS2                       0x80000000000043aL
#define SDB__QGMUPAATTRBYFIELDALIAS                        0x80000000000043bL
#define SDB__QGMUPATTRSBYFIELDALIAS3                       0x80000000000043cL
#define SDB__QGMOPTIAGGREGATION_INIT                       0x80000000000043dL
#define SDB__QGMOPTIAGGREGATION_DONE                       0x80000000000043eL
#define SDB__QGMOPTIAGGREGATION__UPDATE2UNIT               0x80000000000043fL
#define SDB__QGMOPTIAGGREGATION__ADDFIELDS                 0x800000000000440L
#define SDB__QGMOPTIAGGREGATION__GETFIELDALIAS             0x800000000000441L
#define SDB__QGMOPTIAGGREGATION__PUSHOPRUNIT               0x800000000000442L
#define SDB__QGMOPTIAGGREGATION_OURPUTSORT                 0x800000000000443L
#define SDB__QGMOPTIAGGREGATION_PARSE                      0x800000000000444L
#endif
