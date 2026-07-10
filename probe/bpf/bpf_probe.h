/*
 * Minimal, self-contained definitions for the TC flow probe.
 *
 * This header intentionally avoids libbpf and distro UAPI headers so the eBPF
 * object can be compiled with nothing but clang's `bpf` target on any host
 * (no libbpf-dev, no kernel headers). It only declares what tc_monitor needs:
 * fixed-layout packet wire structs, a couple of stable UAPI constants, the two
 * map helpers (by their frozen helper IDs), and the BTF map-definition macros.
 *
 * The program reads only packet data (never internal kernel structs), so it
 * needs no CO-RE relocations and runs across kernel versions, including kernels
 * built without BTF.
 */
#pragma once

typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;

/* Section / inline attributes (subset of libbpf's bpf_helpers.h). */
#define SEC(name) __attribute__((section(name), used))
#define __always_inline inline __attribute__((always_inline))

/* BTF-defined map macros (identical to libbpf's bpf_helpers.h). */
#define __uint(name, val) int(*name)[val]
#define __type(name, val) typeof(val) *name

/* Stable UAPI numeric constants. Map-type values are from enum bpf_map_type:
 *   PERCPU_HASH=5, PERCPU_ARRAY=6, LRU_HASH=9, LRU_PERCPU_HASH=10. */
#define BPF_MAP_TYPE_PERCPU_ARRAY 6
#define BPF_MAP_TYPE_LRU_PERCPU_HASH 10
#define BPF_ANY 0
#define TC_ACT_OK 0
#define ETH_P_IP 0x0800
#define IPPROTO_TCP 6
#define IPPROTO_UDP 17

/* eBPF map helpers, referenced by their stable UAPI helper IDs. */
static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static long (*bpf_map_update_elem)(void *map, const void *key, const void *value,
				   __u64 flags) = (void *)2;

/* Byte-order helpers. clang defines __BYTE_ORDER__ for the target. */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define bpf_htons(x) __builtin_bswap16(x)
#define bpf_ntohs(x) __builtin_bswap16(x)
#define bpf_htonl(x) __builtin_bswap32(x)
#define bpf_ntohl(x) __builtin_bswap32(x)
#else
#define bpf_htons(x) (x)
#define bpf_ntohs(x) (x)
#define bpf_htonl(x) (x)
#define bpf_ntohl(x) (x)
#endif

/*
 * __sk_buff is a UAPI struct whose field offsets are frozen. We only touch
 * len, data and data_end, but the leading fields are kept so their offsets
 * match the kernel exactly.
 */
struct __sk_buff {
	__u32 len;
	__u32 pkt_type;
	__u32 mark;
	__u32 queue_mapping;
	__u32 protocol;
	__u32 vlan_present;
	__u32 vlan_tci;
	__u32 vlan_proto;
	__u32 priority;
	__u32 ingress_ifindex;
	__u32 ifindex;
	__u32 tc_index;
	__u32 cb[5];
	__u32 hash;
	__u32 tc_classid;
	__u32 data;
	__u32 data_end;
};

#define ETH_ALEN 6
struct ethhdr {
	__u8 h_dest[ETH_ALEN];
	__u8 h_source[ETH_ALEN];
	__u16 h_proto;
} __attribute__((packed));

struct iphdr {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	__u8 ihl : 4;
	__u8 version : 4;
#else
	__u8 version : 4;
	__u8 ihl : 4;
#endif
	__u8 tos;
	__u16 tot_len;
	__u16 id;
	__u16 frag_off;
	__u8 ttl;
	__u8 protocol;
	__u16 check;
	__u32 saddr;
	__u32 daddr;
} __attribute__((packed));

/* Full 20-byte layout so a `(tcp + 1)` bounds check requires a complete header. */
struct tcphdr {
	__u16 source;
	__u16 dest;
	__u32 seq;
	__u32 ack_seq;
	__u16 flags;
	__u16 window;
	__u16 check;
	__u16 urg_ptr;
} __attribute__((packed));

struct udphdr {
	__u16 source;
	__u16 dest;
	__u16 len;
	__u16 check;
} __attribute__((packed));
