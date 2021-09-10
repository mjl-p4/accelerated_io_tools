ifeq ($(SCIDB),)
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
endif

CFLAGS  := -DARROW_NO_DEPRECATED_API -DNDEBUG -DUSE_ARROW -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -O3 -Wall -Wextra -Wno-long-long -Wno-strict-aliasing -Wno-system-headers -Wno-unused -Wno-unused-parameter -Wno-variadic-macros -fPIC -fno-omit-frame-pointer -g -std=c++17
INC     := -I. -DPROJECT_ROOT="\"$(SCIDB)\""  -I"$(SCIDB)/include"  -I"/usr/pgsql-9.3/include" -I"/usr/include/boost169"
LIBS    :=-shared -Wl,-soname,libaccelerated_io_tools.so -L. -L"$(SCIDB)/lib" -Wl,-rpath,$(SCIDB)/lib -lm -larrow
SRCS    := plugin.cpp LogicalSplit.cpp PhysicalSplit.cpp LogicalParse.cpp PhysicalParse.cpp LogicalAioInput.cpp PhysicalAioInput.cpp LogicalAioSave.cpp PhysicalAioSave.cpp Functions.cpp
HEADERS := AioInputSettings.h AioSaveSettings.h ParseSettings.h SplitSettings.h UnparseTemplateParser.h
OBJS    := $(SRCS:%.cpp=%.o)

ifneq ("$(wildcard /opt/apache-arrow/lib64)","")
  # -- - CentOS - --
  ARROW_PATH := /opt/apache-arrow
  INC     += -I$(ARROW_PATH)/include
  LIBS    += -L$(ARROW_PATH)/lib64 -Wl,-rpath,$(ARROW_PATH)/lib64
  $(info Using Apache Arrow in $(ARROW_PATH))
endif

# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
  CC  := "/usr/bin/gcc-4.9"
  CXX := "/usr/bin/g++-4.9"
else
  ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
   CC  := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
   CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
  endif
endif


%.o: %.cpp $(HEADERS)
	$(CXX) $(CFLAGS) $(INC) -c $< -o $@

all: libaccelerated_io_tools.so

libaccelerated_io_tools.so: $(OBJS)
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CFLAGS) $(INC) -o libaccelerated_io_tools.so $(OBJS) $(LIBS)
	@echo "Now copy *.so to your SciDB lib/scidb/plugins directory and run"
	@echo "iquery -aq \"load_library('accelerated_io_tools')\" # to load the plugin."
	@echo
	@echo "Re-start SciDB if the plugin was already loaded previously."
	@echo "Remember to copy the plugin to all your nodes in the cluster."

clean:
	rm -f *.so *.o
