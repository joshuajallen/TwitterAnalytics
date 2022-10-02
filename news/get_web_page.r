#!/usr/bin/env Rscript
args = commandArgs(trailingOnly = TRUE)
options(warn=-1)
write(readLines(args[1]), stdout())