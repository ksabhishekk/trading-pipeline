import struct
import sys
import matplotlib.pyplot as plt

# The Tick struct layout is:
# uint64_t seq; (8 bytes)
# uint64_t gen_timestamp_ns; (8 bytes)
# double price; (8 bytes)
# int32_t symbol_id; (4 bytes)
# Total: 28 bytes
TICK_FMT = '<QQdi'
TICK_SIZE = struct.calcsize(TICK_FMT)

def main():
    filename = 'processed_ticks.bin'
    try:
        with open(filename, 'rb') as f:
            data = f.read()
    except FileNotFoundError:
        print(f"Error: {filename} not found. Did the feed_handler run successfully?")
        sys.exit(1)
        
    num_ticks = len(data) // TICK_SIZE
    print(f"Read {num_ticks} ticks from {filename}")
    
    seqs = []
    prices = []
    
    for i in range(num_ticks):
        chunk = data[i * TICK_SIZE : (i + 1) * TICK_SIZE]
        seq, gen_ts, price, sym = struct.unpack(TICK_FMT, chunk)
        seqs.append(seq)
        prices.append(price)
        
    plt.figure(figsize=(10, 5))
    plt.plot(seqs, prices, marker='o', markersize=1, linestyle='-', linewidth=0.5, color='blue')
    plt.title('Simulated Market Data (Price vs Sequence Number)')
    plt.xlabel('Sequence Number')
    plt.ylabel('Price')
    plt.grid(True)
    
    output_img = 'market_data_plot.png'
    plt.savefig(output_img, dpi=150)
    print(f"Plot saved successfully to {output_img}!")

if __name__ == '__main__':
    main()
