//go:build ignore

#include "bpf_probe.h"

// more fragments constant from IP header
#ifndef IP_MF
#define IP_MF 0x2000
#endif

#ifndef IP_OFFSET
#define IP_OFFSET 0x1fff
#endif

// Will add padding for golang struct alignment
// Total is 16 bytes
struct flow_key {
    __u32 src_ip;   // 4
    __u32 dst_ip;   // 4
    __u16 src_port; // 2
    __u16 dst_port; // 2
    __u8 proto;     // 1
    __u8 pad[3];    // 3
};

struct flow_stats {
    __u64 packets; // 8
    __u64 bytes;   // 8
};

enum stats_key {
    STAT_NEW_FLOW_INSERT_ATTEMPTS = 0,
    STAT_FLOW_INSERT_FAILURES = 1,
    STAT_MAX = 2,
};

#define DEFAULT_FLOW_MAP_MAX_ENTRIES 10000

struct {
    __uint(type, BPF_MAP_TYPE_LRU_PERCPU_HASH);
    // Default for the ELF spec. Userspace overrides this from -max-flows before loading.
    __uint(max_entries, DEFAULT_FLOW_MAP_MAX_ENTRIES);
    __type(key, struct flow_key);
    __type(value, struct flow_stats);
} flow_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, STAT_MAX);
    __type(key, __u32);
    __type(value, __u64);
} stats_map SEC(".maps");

static __always_inline void incr_stat(__u32 key) {
    __u64 *counter = bpf_map_lookup_elem(&stats_map, &key);
    if (counter)
        *counter += 1;
}

SEC("classifier") // Note: "classifier" is used for TC programs
int tc_monitor(struct __sk_buff *skb) {
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return TC_ACT_OK;

    if (eth->h_proto != bpf_htons(ETH_P_IP))
        return TC_ACT_OK;

    struct iphdr *ip = data + sizeof(struct ethhdr);
    if ((void *)(ip + 1) > data_end)
        return TC_ACT_OK;

    if (ip->version != 4)
        return TC_ACT_OK;

    __u32 ip_hdr_len = ip->ihl * 4;
    if (ip_hdr_len < sizeof(*ip))
        return TC_ACT_OK;

    if ((void *)ip + ip_hdr_len > data_end)
        return TC_ACT_OK;

    // Ports are only present in the first fragment.
    if (ip->frag_off & bpf_htons(IP_MF | IP_OFFSET))
        return TC_ACT_OK;

    __u16 src_port = 0;
    __u16 dst_port = 0;
    void *transport = (void *)ip + ip_hdr_len;

    if (ip->protocol == IPPROTO_TCP) {
        struct tcphdr *tcp = transport;
        if ((void *)(tcp + 1) > data_end)
            return TC_ACT_OK;
        src_port = bpf_ntohs(tcp->source);
        dst_port = bpf_ntohs(tcp->dest);
    } else if (ip->protocol == IPPROTO_UDP) {
        struct udphdr *udp = transport;
        if ((void *)(udp + 1) > data_end)
            return TC_ACT_OK;
        src_port = bpf_ntohs(udp->source);
        dst_port = bpf_ntohs(udp->dest);
    } else {
        return TC_ACT_OK;
    }

    struct flow_key key = {
        .src_ip = bpf_ntohl(ip->saddr),
        .dst_ip = bpf_ntohl(ip->daddr),
        .src_port = src_port,
        .dst_port = dst_port,
        .proto  = ip->protocol,
        .pad = {0, 0, 0},
    };

    struct flow_stats *stats = bpf_map_lookup_elem(&flow_map, &key);
    if (!stats) {
        struct flow_stats zero = {0};
        incr_stat(STAT_NEW_FLOW_INSERT_ATTEMPTS);
        if (bpf_map_update_elem(&flow_map, &key, &zero, BPF_ANY) < 0)
            incr_stat(STAT_FLOW_INSERT_FAILURES);
        stats = bpf_map_lookup_elem(&flow_map, &key);
    }

    if (stats) {
        stats->packets += 1;
        stats->bytes += skb->len;
    }

    return TC_ACT_OK; // Continue packet processing
}

char _license[] SEC("license") = "GPL";
