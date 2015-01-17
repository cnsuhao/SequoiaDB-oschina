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
#define SDB__QGMBUILDER_BUILDORDERBY                       0x80000000000080eL
#define SDB__QGMBUILDER_BUILD1                             0x80000000000080fL
#define SDB__QGMBUILDER_BUILD2                             0x800000000000810L
#define SDB__QGMBUILDER__BUILDPHYNODE                      0x800000000000811L
#define SDB__QGMBUILDER__ADDPHYCOMMAND                     0x800000000000812L
#define SDB__QGMBUILDER__ADDPHYAGGR                        0x800000000000813L
#define SDB__QGMBUILDER__CRTPHYSORT                        0x800000000000814L
#define SDB__QGMBUILDER__ADDPHYSCAN                        0x800000000000815L
#define SDB__QGMBUILDER__ADDMTHMATHERSCAN                  0x800000000000816L
#define SDB__QGMBUILDER__CRTPHYJOIN                        0x800000000000817L
#define SDB__QGMBUILDER__CRTPHYFILTER                      0x800000000000818L
#define SDB__QGMBUILDER__CRTMTHMATHERFILTER                0x800000000000819L
#define SDB__QGMBUILDER__BUILD1                            0x80000000000081aL
#define SDB__QGMBUILDER__BUILDUPDATE                       0x80000000000081bL
#define SDB__QGMBUILDER__ADDSET                            0x80000000000081cL
#define SDB__QGMBUILDER__BUILDDELETE                       0x80000000000081dL
#define SDB__QGMBUILDER__BUILDDROPCL                       0x80000000000081eL
#define SDB__QGMBUILDER__BUILDDROPINX                      0x80000000000081fL
#define SDB__QGMBUILDER__BUILDCRTINX                       0x800000000000820L
#define SDB__QGMBUILDER__BUILDINXCOLUMNS                   0x800000000000821L
#define SDB__QGMBUILDER__BUILDCRTCL                        0x800000000000822L
#define SDB__QGMBUILDER__BUILDCRTCS                        0x800000000000823L
#define SDB__QGMBUILDER__BUILDDROPCS                       0x800000000000824L
#define SDB__QGMBUILDER__BUILDSELECT                       0x800000000000825L
#define SDB__QGMBUILDER__BUILDINSERT                       0x800000000000826L
#define SDB__QGMBUILDER__ADDSELECTOR                       0x800000000000827L
#define SDB__QGMBUILDER__ADDFROM                           0x800000000000828L
#define SDB__QGMBUILDER__BUILDJOIN                         0x800000000000829L
#define SDB__QGMBUILDER__BUILDINCONDITION                  0x80000000000082aL
#define SDB__QGMBUILDER__BUILDCONDITION                    0x80000000000082bL
#define SDB__QGMBUILDER__ADDSPLITBY                        0x80000000000082cL
#define SDB__QGMBUILDER__ADDGROUPBY                        0x80000000000082dL
#define SDB__QGMBUILDER__ADDORDERBY                        0x80000000000082eL
#define SDB__QGBUILDER__ADDLIMIT                           0x80000000000082fL
#define SDB__QGMBUILDER__ADDSKIP                           0x800000000000830L
#define SDB__QGMBUILDER__ADDCOLUMNS                        0x800000000000831L
#define SDB__QGMBUILDER__ADDVALUES                         0x800000000000832L
#define SDB__QGMBUILDER__ADDHINT                           0x800000000000833L
#define SDB__QGMEXTENDSELECTPLAN__EXTEND                   0x800000000000834L
#define SDB__QGMOPTISORT_INIT                              0x800000000000835L
#define SDB__QGMOPTISORT__PUSHOPRUNIT                      0x800000000000836L
#define SDB__QGMOPTISORT__RMOPRUNIT                        0x800000000000837L
#define SDB__QGMOPTISORT_APPEND                            0x800000000000838L
#define SDB__QGMFETCHOUT_ELEMENT                           0x800000000000839L
#define SDB__QGMFETCHOUT_ELEMENTS                          0x80000000000083aL
#define SDB__QGMPLMTHMATCHERSCAN__EXEC                     0x80000000000083bL
#define SDB__QGMSELECTOR_SELECT                            0x80000000000083cL
#define SDB__QGMSELECTOR_SELECT2                           0x80000000000083dL
#define SDB__QGMPLHASHJOIN_INIT                            0x80000000000083eL
#define SDB__QGMPLHASHJOIN__EXEC                           0x80000000000083fL
#define SDB__QGMPLHASHJOIN__FETCHNEXT                      0x800000000000840L
#define SDB__QGMPLHASHJOIN__BUILDHASNTBL                   0x800000000000841L
#define SDB__QGMPLSORT__EXEC                               0x800000000000842L
#define SDB__QGMPLSORT__FETCHNEXT                          0x800000000000843L
#define SDB__QGMPLINSERT__MERGEOBJ                         0x800000000000844L
#define SDB__QGMPLINSERT__NEXTRECORD                       0x800000000000845L
#define SDB__QGMPLINSERT__EXEC                             0x800000000000846L
#define SDB__QGMPLUPDATE__EXEC                             0x800000000000847L
#define SDB__QGMOPTIAGGREGATION_INIT                       0x800000000000848L
#define SDB__QGMOPTIAGGREGATION_DONE                       0x800000000000849L
#define SDB__QGMOPTIAGGREGATION__UPDATE2UNIT               0x80000000000084aL
#define SDB__QGMOPTIAGGREGATION__ADDFIELDS                 0x80000000000084bL
#define SDB__QGMOPTIAGGREGATION__GETFIELDALIAS             0x80000000000084cL
#define SDB__QGMOPTIAGGREGATION__PUSHOPRUNIT               0x80000000000084dL
#define SDB__QGMOPTIAGGREGATION_OURPUTSORT                 0x80000000000084eL
#define SDB__QGMOPTIAGGREGATION_PARSE                      0x80000000000084fL
#define SDB__QGMOPTISELECT_INIT                            0x800000000000850L
#define SDB__QGMOPTISELECT_DONE                            0x800000000000851L
#define SDB__QGMOPTISELECT_OUTPUTSTREAM                    0x800000000000852L
#define SDB__QGMOPTISELECT__PUSHOPRUNIT                    0x800000000000853L
#define SDB__QGMOPTISELECT__RMOPRUNIT                      0x800000000000854L
#define SDB__QGMOPTISELECT__EXTEND                         0x800000000000855L
#define SDB__QGMOPTISELECT__VALIDATEANDCRTPLAN             0x800000000000856L
#define SDB__QGMOPTISELECT__PARAMEXISTINSELECOTR           0x800000000000857L
#define SDB__QGMPLSCAN__EXEC                               0x800000000000858L
#define SDB__QGMPLSCAN__EXECONDATA                         0x800000000000859L
#define SDB__QGMPLSCAN__EXECONCOORD                        0x80000000000085aL
#define SDB__QGMPLSCAN__FETCHNEXT                          0x80000000000085bL
#define SDB__QGMPLSCAN__FETCH                              0x80000000000085cL
#define SDB__QGMPLMTHMATCHERFILTER__FETCHNEXT              0x80000000000085dL
#define SDB__QGMEXTENDPLAN_EXTEND                          0x80000000000085eL
#define SDB__QGMEXTENDPLAN_INSERTPLAN                      0x80000000000085fL
#define SDB__QGMPLSPLITBY__FETCHNEXT                       0x800000000000860L
#define SDB__QGMPLFILTER__FETCHNEXT                        0x800000000000861L
#define SDB__QGMPARAMTABLE_ADDCONST                        0x800000000000862L
#define SDB__QGMPARAMTABLE_ADDCONST2                       0x800000000000863L
#define SDB__QGMPARATABLE_ADDVAR                           0x800000000000864L
#define SDB__QGMPARAMTABLE_SETVAR                          0x800000000000865L
#define SDB__QGMPLDELETE__EXEC                             0x800000000000866L
#define SDB__QGMOPTINLJOIN__MAKECONDVAR                    0x800000000000867L
#define SDB__QGMOPTINLJOIN_MAKECONDITION                   0x800000000000868L
#define SDB__QGMOPTINLJOIN_INIT                            0x800000000000869L
#define SDB__QGMOPTINLJOIN__CRTJOINUNIT                    0x80000000000086aL
#define SDB__QGMOPTINLJOIN_OUTPUTSTREAM                    0x80000000000086bL
#define SDB__QGMOPTINLJOIN__PUSHOPRUNIT                    0x80000000000086cL
#define SDB__QGMOPTINLJOIN__UPDATECHANGE                   0x80000000000086dL
#define SDB__QGMOPTINLJOIN_HANDLEHINTS                     0x80000000000086eL
#define SDB__QGMOPTINLJOIN__VALIDATE                       0x80000000000086fL
#define SDB__QGMPLNLJOIN__INIT                             0x800000000000870L
#define SDB__QGMPLNLJOIN__EXEC                             0x800000000000871L
#define SDB__QGMPLNLJOIN__FETCHNEXT                        0x800000000000872L
#define SDB__QGMPLNLJOIN__MODIFYINNERCONDITION             0x800000000000873L
#define SDB__QGMOPSTREAM_FIND                              0x800000000000874L
#define SDB__QGMOPRUNIT_ADDOPFIELD                         0x800000000000875L
#define SDB__QGMOPTITREENODE_ADDCHILD                      0x800000000000876L
#define SDB__QGMOPTITREENODE__ONPUSHOPRUNIT                0x800000000000877L
#define SDB__QGMOPTITREENODE_PUSHOPRUNIT                   0x800000000000878L
#define SDB__QGMOPTITREENODE_RMOPRUNIT                     0x800000000000879L
#define SDB__QGMOPTITREENODE_UPCHANGE                      0x80000000000087aL
#define SDB__QGMOPTITREENODE_OUTPUTSORT                    0x80000000000087bL
#define SDB__QGMOPTITREENODE_EXTEND                        0x80000000000087cL
#define SDB__QGMOPTTREE__PREPARE                           0x80000000000087dL
#define SDB__QGMOPTTREE_INSERTBETWEEN                      0x80000000000087eL
#define SDB__QGMUTILFIRSTDOT                               0x80000000000087fL
#define SDB__QGMFINDFIELDFROMFUNC                          0x800000000000880L
#define SDB__QGMISFROMONE                                  0x800000000000881L
#define SDB__QGMISSAMEFROM                                 0x800000000000882L
#define SDB__QGMMERGE                                      0x800000000000883L
#define SDB__QGMREPLACEFIELDRELE                           0x800000000000884L
#define SDB__QGMREPLACEATTRRELE                            0x800000000000885L
#define SDB__QGMREPLACEATTRRELE2                           0x800000000000886L
#define SDB__QGMREPLACEATTRRELE3                           0x800000000000887L
#define SDB__QDMDOWNFIELDSBYFIELDALIAS                     0x800000000000888L
#define SDB__QGMDOWNATTRSBYFIELDALIAS                      0x800000000000889L
#define SDB__QGMDOWNATTRSBYFIELDALIAS2                     0x80000000000088aL
#define SDB__QGMDOWNAATTRBYFIELDALIAS                      0x80000000000088bL
#define SDB__QGMDOWNAGGRSBYFIELDALIAS                      0x80000000000088cL
#define SDB__QGMUPFIELDSBYFIELDALIAS                       0x80000000000088dL
#define SDB__QGMUPATTRSBYFIELDALIAS                        0x80000000000088eL
#define SDB__QGMUPATTRSBYFIELDALIAS2                       0x80000000000088fL
#define SDB__QGMUPAATTRBYFIELDALIAS                        0x800000000000890L
#define SDB__QGMUPATTRSBYFIELDALIAS3                       0x800000000000891L
#define SDB__QGMPLCOMMAND__EXEC                            0x800000000000892L
#define SDB__QGMPLCOMMAND_EXECONCOORD                      0x800000000000893L
#define SDB__QGMPLCOMMAND__EXECONDATA                      0x800000000000894L
#define SDB__QGMPLCOMMAND__FETCHNEXT                       0x800000000000895L
#define SDB__QGMCONDITIONNODEHELPER_MERGE                  0x800000000000896L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE               0x800000000000897L
#define SDB__QGMCONDITIONNODEHELPER_SEPARATE2              0x800000000000898L
#define SDB__QGMCONDITIONNODEHELPER__CRTBSON               0x800000000000899L
#define SDB__QGMCONDITIONNODEHELPER__TOBSON                0x80000000000089aL
#define SDB__QGMCONDITIONNODEHELPER__GETALLATTR            0x80000000000089bL
#define SDB__QGMHASHTBL_PUSH                               0x80000000000089cL
#define SDB__QGMHASHTBL_FIND                               0x80000000000089dL
#define SDB__QGMHASHTBL_GETMORE                            0x80000000000089eL
#define SDB__QGMPLAN_EXECUTE                               0x80000000000089fL
#define SDB__QGMPLAN_FETCHNEXT                             0x8000000000008a0L
#define SDB__QGMPTRTABLE_GETFIELD                          0x8000000000008a1L
#define SDB__QGMPTRTABLE_GETOWNFIELD                       0x8000000000008a2L
#define SDB__QGMPTRTABLE_GETATTR                           0x8000000000008a3L
#define SDB__QGMMATCHER_MATCH                              0x8000000000008a4L
#define SDB__QGMMATCHER__MATCH                             0x8000000000008a5L
#endif
