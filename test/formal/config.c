/* CBMC entry points include production code; no rewritten implementation. */
#include "../../native/j2o_bridge.c"
#include <assert.h>
#include <stdarg.h>

extern uint32_t nondet_u32(void);
extern uint64_t nondet_u64(void);
extern int nondet_int(void);
extern unsigned char nondet_byte(void);
extern void __CPROVER_assume(__CPROVER_bool);

/* CBMC 6.11 has no strnlen body. Model the standard bounded libc operation;
 * bridge functions remain the production definitions included above. */
size_t strnlen(const char *text, size_t bound)
{
    size_t length = 0;
    while (length < bound && text[length] != 0) ++length;
    return length;
}

/* The bundled vsnprintf model can omit the required trailing NUL. Model the
 * sole formatter used by this translation unit exactly, with an assertion
 * rejecting any future format change instead of silently generalizing it. */
int snprintf(char *out, size_t capacity, const char *format, ...)
{
    assert(strcmp(format, "%llu") == 0);
    va_list arguments;
    va_start(arguments, format);
    unsigned long long value = va_arg(arguments, unsigned long long);
    va_end(arguments);
    char reverse[20];
    size_t length = 0;
    do { reverse[length++] = (char)('0' + value % 10); value /= 10; } while (value != 0);
    if (capacity != 0) {
        size_t copied = length < capacity - 1 ? length : capacity - 1;
        for (size_t i = 0; i < copied; ++i) out[i] = reverse[length - i - 1];
        out[copied] = 0;
    }
    return (int)length;
}

static void terminated(const j2o_error *error)
{
    assert(error->abi_size == sizeof(*error));
    assert(error->message[255] == 0 && error->syscall[31] == 0);
    assert(error->errno_name[31] == 0 && error->key[63] == 0 && error->given[127] == 0);
}
static void cleared(const j2o_error *error)
{
    const unsigned char *bytes = (const unsigned char *)error;
    for (size_t i = 0; i < sizeof(*error); ++i) assert(bytes[i] == 0);
}
void verify_numeric(void)
{
    j2o_error error;
    j2o_config *config = j2o_config_create(&error);
    if (config == NULL) { assert(error.result == J2O_NOMEM); terminated(&error); return; }
    uint32_t key = nondet_u32();
    uint64_t value = nondet_u64();
    uint64_t before[15]; memcpy(before, config->values, sizeof(before));
    int accepted = (key == J2O_PORT && value <= 65535) ||
        (key >= J2O_MAX_REQUEST_BYTES && key <= J2O_HANDSHAKE_TIMEOUT_MS &&
         (value != 0 || key == J2O_REQUEST_TIMEOUT_MS || key == J2O_WRITE_TIMEOUT_MS) &&
         (key > J2O_MAX_CONNECTIONS || value <= SIZE_MAX));
    j2o_result result = j2o_config_set_u64(config, key, value, &error);
    assert((result == J2O_OK) == accepted);
    if (accepted) { assert(config->values[key] == value); cleared(&error); }
    else { assert(error.result == J2O_INVALID); terminated(&error); assert(memcmp(before, config->values, sizeof(before)) == 0); }
    int boolean = nondet_int(), previous = config->reuse_port;
    result = j2o_config_set_bool(config, key, boolean, &error);
    if (key != J2O_REUSE_PORT || (boolean != 0 && boolean != 1)) {
        assert(result == J2O_INVALID && config->reuse_port == previous); terminated(&error);
    } else {
#ifdef SO_REUSEPORT
        assert(result == J2O_OK && config->reuse_port == boolean); cleared(&error);
#else
        assert(boolean ? result == J2O_UNSUPPORTED : result == J2O_OK);
#endif
    }
    assert(j2o_config_set_u64(config, key, value, NULL) == J2O_INVALID);
    assert(j2o_config_set_u64(NULL, key, value, &error) == J2O_INVALID);
    terminated(&error);
    j2o_config_destroy(config);
}
void verify_host(void)
{
    j2o_error error;
    j2o_config *config = j2o_config_create(&error);
    if (config == NULL) { assert(error.result == J2O_NOMEM); terminated(&error); return; }
    char bytes[4];
    for (size_t i = 0; i < sizeof(bytes); ++i) bytes[i] = (char)nondet_byte();
    size_t length = nondet_u32(); __CPROVER_assume(length <= sizeof(bytes));
    char *previous = config->host;
    int valid = length != 0;
    for (size_t i = 0; i < length; ++i) if (bytes[i] == 0) valid = 0;
    j2o_result result = j2o_config_set_string(config, J2O_HOST, bytes, length, &error);
    if (!valid) assert(result == J2O_INVALID);
    else assert(result == J2O_OK || result == J2O_NOMEM);
    if (result == J2O_OK) {
        assert(config->host[length] == 0);
        assert(memcmp(config->host, bytes, length) == 0);
        memset(bytes, 0, sizeof(bytes)); /* Native storage is independent of caller storage. */
        cleared(&error);
    } else {
        assert(config->host == previous && strcmp(config->host, "127.0.0.1") == 0);
        terminated(&error);
    }
    j2o_config_destroy(config);
}
void verify_errors(void)
{
    char text[300];
    for (size_t i = 0; i + 1 < sizeof(text); ++i) {
        text[i] = (char)nondet_byte(); __CPROVER_assume(text[i] != 0);
    }
    text[299] = 0;
    j2o_error error;
    assert(fail(&error, J2O_INVALID, 22, text, text, text, text, text) == J2O_INVALID);
    terminated(&error);
    assert(error.result == J2O_INVALID && error.os_errno == 22);
    assert(memcmp(error.message, text, 255) == 0 && memcmp(error.syscall, text, 31) == 0);
    assert(memcmp(error.errno_name, text, 31) == 0 && memcmp(error.key, text, 63) == 0);
    assert(memcmp(error.given, text, 127) == 0);
    j2o_error saved; memcpy(&saved, &error, sizeof(saved));
    memset(text, 0, sizeof(text)); success(&error);
    assert(saved.message[0] != 0 && saved.given[0] != 0); /* No borrowed string storage. */
    cleared(&error);
}
