#!/bin/sh

# Print document metadata
cat << \EOT
---
title: Ugrid Reference
geometry: margin=1in
header-includes:
 - \usepackage{fancyhdr}
 - \pagestyle{fancy}
 - \fancyfoot[LE,LO]{v0.1.1}
---

\pagebreak

EOT

awk '
# Avoid doctoc TOC
/START doctoc/ {intoc = 1; next}
/END doctoc/ {intoc = 0; next}
intoc == 1 {next}

# Fix sections
/^#/ {
	if ($1 == "#") next
	if ($2 == "References") inref = 1
	sub(/#/, "", $1)
	print
	next
}

# Fix references
inref == 1 && /: / {
	print $0; print ""
	sub(/: /, " ", $0)
}

# otherwise print
{print}
' "$@"
