# import plotly as py   # plotly.plotly as py 
# import plotly.graph_objs as go

# avg_laten = [0, 0, 0,  0, 0, 0,  0, 0, 0] 
# lt_size   = [0, 0, 0,  0, 0, 0,  0, 0, 0]  


# errors = 0
# new_rows = 0
# new_columns = 0
# avg_latency = 0.0 
# total_size = 0.0
# 0 ["us1"], 1 ["us10"], 2 ["us100"], 3 ["ms1"], 4 ["ms10"], 5 ["ms100"], 6 ["s1"], 7 ["s110"], 8 ["s11_greater"]
# avg_laten = [0, 0, 0,  0, 0, 0,  0, 0, 0]
# lt_size   = [0, 0, 0,  0, 0, 0,  0, 0, 0]


                # print("=====================\nMetrics Results data:\n=====================")
                # output =  "New columns : " + str(new_columns) +"\n"+ "New rows : "+ str(new_rows) +"\n"+ \
                #           "Average latency: " + str(avg_latency) +" seconds to execute 1 item on average." +"\n"+ \
                #           "Lantency of Throughputs (items/timeframe) : " + str(avg_laten) +"\n"+ \
                #           "Cummulative latency sizes (Megabytes) : " + str(lt_size)
                # print( output )

                # plotHistogram (avg_laten, lt_size)

                
def recordMetrics(elapse_time, item_size):
    global avg_laten
    global lt_size

    # Number of events executed within timeframes: 0-1us, 2-10us, 11-100us   (micro-seconds)
    if elapse_time <= 1*10**(-6):
        avg_laten[ 0 ] += 1
        lt_size  [ 0 ] += item_size
    elif elapse_time > 1*10**(-6) and elapse_time <= 1*10**(-5):
        avg_laten[ 1 ] += 1
        lt_size  [ 1 ] += item_size
    elif elapse_time > 1*10**(-5) and elapse_time <= 1*10**(-4):
        avg_laten[ 2 ] += 1
        lt_size  [ 2 ] += item_size
    # Number of events executed within timeframes: 101us-1ms, 2-10ms, 11-100ms   (micro-mili-seconds)
    elif elapse_time > 1*10**(-4) and elapse_time <= 0.001:
        avg_laten[ 3 ] += 1
        lt_size  [ 3 ] += item_size
    elif elapse_time >0.001 and elapse_time <= 0.01:
        avg_laten[ 4 ] += 1
        lt_size  [ 4 ] += item_size
    elif elapse_time > 0.01 and elapse_time <= 0.1:
        avg_laten[ 5 ] += 1
        lt_size  [ 5 ] += item_size
    # Number of events executed within timeframes: 100ms-1s, 2s-10s, 11s+    (seconds)
    elif elapse_time > 0.1 and elapse_time <= 1.0:
        avg_laten[ 6 ] += 1
        lt_size  [ 6 ] += item_size
    elif elapse_time > 1.0 and elapse_time <= 10.0:
        avg_laten[ 7 ] += 1
        lt_size  [ 7 ] += item_size
    elif elapse_time >= 11.0:
        avg_laten[ 8 ] += 1
        lt_size  [ 8 ] += item_size

#==================
# Plotting Latency
def plotHistogram (avg_laten, lt_size): 
    x_labels = ['(0-1us)', '(2-10us)', '(11-100us)', '(101us-1ms)', '(2-10ms)', '(11-100ms)', '(100ms-1s)', '(2s-10s)', '(11s +)' ]

    trace1 = go.Bar(x=x_labels, y=avg_laten, name='Items', marker=dict(color='rgb(55, 83, 109)') )

    trace2 = go.Bar(x=x_labels, y=lt_size, name='Size (MB)', marker=dict(color='rgb(26, 118, 255)') )

    data = [trace1, trace2]

    layout = go.Layout (
        title ='Cumulative Distribution of Latency and Throughtput',
        xaxis =dict(title='Latency (of Throughtputs)', tickfont=dict(size=14, color='rgb(107, 107, 107)')),
        yaxis =dict(title='Cummulative Items / Size' , titlefont=dict(size=16, color='rgb(107, 107, 107)'), 
                    tickfont=dict(size=14, color='rgb(107, 107, 107)')),
                    legend=dict(x=0, y=1.0, bgcolor='rgba(255, 255, 255, 0)', bordercolor='rgba(255, 255, 255, 0)'),
                    barmode='group', bargap=0.15, bargroupgap=0.1)

    fig = go.Figure(data=data, layout=layout)
    py.offline.plot(fig, filename='in_metric_plot.html')