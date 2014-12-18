/*   $Id: main.h,v 1.15 2011/10/06 22:29:12 kristaps Exp $ */
/*
 * Copyright (c) 2009, 2010, 2011 Kristaps Dzonsons <kristaps@bsd.lv>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#ifndef   PARSEMANDOC_H
#define   PARSEMANDOC_H

#include "mandoc.h"
#include "mdoc.h"
#include "man.h"

__BEGIN_DECLS

struct   mdoc;
struct   man;

#define   UNCONST(a)   ((void *)(uintptr_t)(const void *)(a))


typedef   void      (*out_mdoc)(void *, const struct mdoc *);
typedef   void      (*out_man)(void *, const struct man *);
typedef   void      (*out_free)(void *);

enum   outt {
   OUTT_ASCII = 0,   /* -Tascii */
   OUTT_LOCALE,   /* -Tlocale */
   OUTT_UTF8,   /* -Tutf8 */
   OUTT_TREE,   /* -Ttree */
   OUTT_MAN,   /* -Tman */
   OUTT_HTML,   /* -Thtml */
   OUTT_XHTML,   /* -Txhtml */
   OUTT_LINT,   /* -Tlint */
   OUTT_PS,   /* -Tps */
   OUTT_PDF   /* -Tpdf */
};

struct   curparse {
   struct mparse    *mp;
   enum mandoclevel  wlevel;   /* ignore messages below this */
   int        wstop;   /* stop after a file with a warning */
   enum outt     outtype;    /* which output to use */
   out_mdoc     outmdoc;   /* mdoc output ptr */
   out_man          outman;   /* man output ptr */
   out_free     outfree;   /* free output ptr */
   void       *outdata;   /* data for output */
   char        outopts[BUFSIZ]; /* buf of output opts */
};


  int        moptions(enum mparset *, char *);
   void        mmsg(enum mandocerr, enum mandoclevel,
            const char *, int, int, const char *);
   void        parse(struct curparse *, int,
            const char *, enum mandoclevel *);
   int        toptions(struct curparse *, char *);
   int        woptions(struct curparse *, char *);



/*
 * Definitions for main.c-visible output device functions, e.g., -Thtml
 * and -Tascii.  Note that ascii_alloc() is named as such in
 * anticipation of latin1_alloc() and so on, all of which map into the
 * terminal output routines with different character settings.
 */

void       *html_alloc(char *);
void       *xhtml_alloc(char *);
void        html_mdoc(void *, const struct mdoc *);
void        html_man(void *, const struct man *);
void        html_free(void *);

void        tree_mdoc(void *, const struct mdoc *);
void        tree_man(void *, const struct man *);

void        man_mdoc(void *, const struct mdoc *);
void        man_man(void *, const struct man *);

void       *locale_alloc(char *);
void       *utf8_alloc(char *);
void       *ascii_alloc(char *);
void        ascii_free(void *);

void       *pdf_alloc(char *);
void       *ps_alloc(char *);
void        pspdf_free(void *);

void        terminal_mdoc(void *, const struct mdoc *);
void        terminal_man(void *, const struct man *);

__END_DECLS


#endif /*!PARSEMANDOC_H*/
