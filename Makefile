###############################################################################
#
# A smart Makefile template for GNU/LINUX programming
#
# Author: PRC (ijkxyz AT msn DOT com)
# Date:   2011/06/17
#
# Usage:
#   $ make           Compile and link (or archive)
#   $ make clean     Clean the objectives and target.
###############################################################################

CROSS_COMPILE =
OPTIMIZE := -O2
WARNINGS := -Wall -Werror 
EXTRA_CFLAGS := --std=c++0x -DMULTITHREAD_SYNC
LIBS := -lboost_thread -lboost_filesystem -lboost_system -lpthread -lleveldb -lhdr_histogram

LIB_DIR   = lib/leveldb/out-shared  lib/HdrHistogram_c-0.9.4/src
INC_DIR   = src  lib/threadpool  lib/leveldb/include  lib/HdrHistogram_c-0.9.4/src
SRC_DIR   =	src  src/coding  src/ds  src/util  src/tests
OBJ_DIR   = src/obj 
EXTRA_SRC = 
EXCLUDE_FILES = 
EXCLUDE_OBJS =

SUFFIX       = c cpp cc cxx
TARGET_DIR   = ./bin
TARGET       := hashkv_test
TARGET_TYPE  := app

ifdef GPROF
    EXTRA_CFLAGS += -pg 
    LDFLAGS += -pg
endif
ifdef DEBUG
    EXTRA_CFLAGS += -g 
    OPTIMIZE := -O0
else
    EXTRA_CFLAGS += -DNDEBUG 
endif

#####################################################################################
#  Do not change any part of them unless you have understood this script very well  #
#  This is a kind remind.                                                           #
#####################################################################################

#FUNC#  Add a new line to the input stream.
define add_newline
$1

endef

#FUNC# set the variable `src-x' according to the input $1
define set_src_x
src-$1 = $(filter-out $4,$(foreach d,$2,$(wildcard $d/*.$1)) $(filter %.$1,$3))

endef

#FUNC# set the variable `obj-x' according to the input $1
define set_obj_x
obj-$1 = $(patsubst %.$1,$3%.o,$(notdir $2))

endef

#VAR# Get the uniform representation of the object directory path name
ifneq ($(OBJ_DIR),)
prefix_objdir  = $(shell echo $(OBJ_DIR)|sed 's:\(\./*\)*::')
prefix_objdir := $(filter-out /,$(prefix_objdir)/)
endif

ifneq ($(TARGET_DIR),)
prefix_bindir  = $(shell echo $(TARGET_DIR)|sed 's:\(\./*\)*::')
prefix_bindir := $(filter-out /,$(prefix_bindir)/)
endif

GCC      := $(CROSS_COMPILE)gcc
G++      := $(CROSS_COMPILE)g++
SRC_DIR := $(sort . $(SRC_DIR))
inc_dir = $(foreach d,$(sort $(INC_DIR) $(SRC_DIR)),-I$d)
lib_dir = $(foreach d,$(LIB_DIR),-L$d)

#--# Do smart deduction automatically
$(eval $(foreach i,$(SUFFIX),$(call set_src_x,$i,$(SRC_DIR),$(EXTRA_SRC),$(EXCLUDE_FILES))))
$(eval $(foreach i,$(SUFFIX),$(call set_obj_x,$i,$(src-$i),$(prefix_objdir))))
$(eval $(foreach f,$(EXTRA_SRC),$(call add_newline,vpath $(notdir $f) $(dir $f))))
$(eval $(foreach d,$(SRC_DIR),$(foreach i,$(SUFFIX),$(call add_newline,vpath %.$i $d))))

EXCLUDE_OBJS = $(foreach t,$(TARGET),$(prefix_objdir)$(notdir $t).o)
all_objs = $(foreach i,$(SUFFIX),$(obj-$i))
all_srcs = $(foreach i,$(SUFFIX),$(src-$i))
share_objs = $(filter-out $(EXCLUDE_OBJS), $(all_objs))

CFLAGS       = $(EXTRA_CFLAGS) $(WARNINGS) $(OPTIMIZE) $(DEFS)
GCCFLAGS	 = $(OPTIMIZE) $(DEFS)
TARGET_TYPE := $(strip $(TARGET_TYPE))

ifeq ($(filter $(TARGET_TYPE),so ar app),)
$(error Unexpected TARGET_TYPE `$(TARGET_TYPE)')
endif

ifeq ($(TARGET_TYPE),so)
 CFLAGS  += -fpic -shared
 LDFLAGS += -shared
endif

PHONY = all .mkdir clean

all: .mkdir $(TARGET)

define cmd_o
$$(obj-$1): $2%.o: %.$1  $(MAKEFILE_LIST)
	@echo $(share_objs)
ifeq ($1,c)
	$(GCC) $(inc_dir) -Wp,-MT,$$@ -Wp,-MMD,$$@.d $(GCCFLAGS) -c -o $$@ $$< -Wno-format
else 
	$(G++) $(inc_dir) -Wp,-MT,$$@ -Wp,-MMD,$$@.d $(CFLAGS) -c -o $$@ $$<
endif

endef

define cmd_t
$1: $(share_objs) $3$(notdir $1).o
	$(G++) $(LDFLAGS) $(lib_dir) $(share_objs) $3$(notdir $(1)).o -o $2$1 $(LIBS)

endef

$(eval $(foreach i,$(SUFFIX),$(call cmd_o,$i,$(prefix_objdir))))
$(eval $(foreach i,$(TARGET),$(call cmd_t,$i,$(prefix_bindir),$(prefix_objdir))))

.mkdir:
	@if [ ! -d $(OBJ_DIR) ]; then mkdir -p $(OBJ_DIR); fi
	@if [ ! -d $(TARGET_DIR) ]; then mkdir -p $(TARGET_DIR); fi

clean:
	rm -f $(prefix_objdir)*.o $(prefix_objdir)*.d $(foreach i,$(TARGET),$(prefix_bindir)$i)

-include $(patsubst %.o,%.o.d,$(share_objs))

.PHONY: $(PHONY)

