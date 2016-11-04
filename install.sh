export GLOBUS_LOCATION=/Users/eyildirim/Documents/globus-6.0/
source $GLOBUS_LOCATION/share/globus-user-env.sh


/usr/bin/gcc -DSTDC_HEADERS=1 -DHAVE_SYS_TYPES_H=1 -DHAVE_SYS_STAT_H=1 -DHAVE_STDLIB_H=1 -DHAVE_STRING_H=1 -DHAVE_MEMORY_H=1 -DHAVE_STRINGS_H=1 -DHAVE_INTTYPES_H=1 -DHAVE_STDINT_H=1 -DHAVE_UNISTD_H=1 -DHAVE_DLFCN_H=1   -I$GLOBUS_LOCATION/include/ -no-cpp-precomp  -g   -m64 -fno-common  -Wall -c general_utility.c general_utility.h

/usr/bin/gcc -DSTDC_HEADERS=1 -DHAVE_SYS_TYPES_H=1 -DHAVE_SYS_STAT_H=1 -DHAVE_STDLIB_H=1 -DHAVE_STRING_H=1 -DHAVE_MEMORY_H=1 -DHAVE_STRINGS_H=1 -DHAVE_INTTYPES_H=1 -DHAVE_STDINT_H=1 -DHAVE_UNISTD_H=1 -DHAVE_DLFCN_H=1    -I$GLOBUS_LOCATION/include/ -no-cpp-precomp  -g   -m64 -fno-common  -Wall -c concurrency_basic_alg.c

/usr/bin/gcc -g -m64 -fno-common -Wall -m64 -o myprogram concurrency_basic_alg.o general_utility.o  $GLOBUS_LOCATION/lib/libglobus_ftp_client.dylib -L$GLOBUS_LOCATION/lib  $GLOBUS_LOCATION/lib/libglobus_ftp_control.dylib $GLOBUS_LOCATION/lib/libglobus_io.dylib $GLOBUS_LOCATION/lib/libglobus_gssapi_error.dylib $GLOBUS_LOCATION/lib/libglobus_gss_assist.dylib $GLOBUS_LOCATION/lib/libglobus_gssapi_gsi.dylib $GLOBUS_LOCATION/lib/libglobus_gsi_proxy_core.dylib $GLOBUS_LOCATION/lib/libglobus_gsi_credential.dylib $GLOBUS_LOCATION/lib/libglobus_gsi_callback.dylib $GLOBUS_LOCATION/lib/libglobus_oldgaa.dylib $GLOBUS_LOCATION/lib/libglobus_gsi_sysconfig.dylib $GLOBUS_LOCATION/lib/libglobus_gsi_cert_utils.dylib $GLOBUS_LOCATION/lib/libglobus_openssl.dylib $GLOBUS_LOCATION/lib/libglobus_xio.dylib $GLOBUS_LOCATION/lib/libglobus_openssl_error.dylib $GLOBUS_LOCATION/lib/libglobus_callout.dylib $GLOBUS_LOCATION/lib/libglobus_proxy_ssl.dylib $GLOBUS_LOCATION/lib/libglobus_common.dylib -lltdl -lm -lssl -lcrypto


