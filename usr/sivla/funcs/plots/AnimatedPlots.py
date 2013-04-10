import sys
print sys.path
from PyQt4 import QtGui

from matplotlib.figure import Figure
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from simep.funcs.data.pyData import pyData
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from datetime import timedelta, datetime 
 
 


class Monitor(FigureCanvas):
    def __init__(self, data, title, timer_length = 100):
        self.fig = Figure()
                
        self.ax = self.fig.add_axes([0.1, 0.3, 0.3, 0.6])
        self.ax.set_title('Limit Orderbook')
        self.ax.set_ylabel('Price')
        #add_subplot(121)
        self.ax_intraday = self.fig.add_axes([0.45, 0.3, 0.5, 0.6], sharey = self.ax)        
        self.ax_intraday.set_title('Intraday Price and Volume' if title is None else title)
        #
        # Optional Title Line (missing security_id)
        #self.ax_intraday.set_title('LobRecorder <%s : %s/%s/%s>' % (data.info['security'], data.info['date'][6:8], data.info['date'][4:6], data.info['date'][0:4]))
        #
        self.ax_volume = self.fig.add_axes([0.45, 0.075, 0.5, 0.175])
        
        self.ax_volume.set_xlabel('Time')
        self.ax_volume.set_ylabel('Volume')
        
        FigureCanvas.__init__(self, self.fig)

        # the file is 5 float values separated by spaces on every line, like this:
        #0.28 0.18 0.18 0.18 0.18
        #0.352 0.162 0.162 0.162 0.162
        #0.4168 0.1458 0.1458 0.1458 0.1458
        #0.37512 0.23122 0.13122 0.13122 0.13122
        self.data = data
        
        self.counter = 1
        
        # the width of the bars
        self.width = self.tick_size(data)/1.01

        # the locations of the bars
        #self.locs = np.arange(len(self.data[0]))

        # the first set of bars Bid-Ask Spread
        self.bars_bid = self.ax.barh(self.data['bid_price'][0], self.data['bid_size'][0], height = self.width, color='#6a7ea6')
        self.bars_ask = self.ax.barh(self.data['ask_price'][0], self.data['ask_size'][0], height = self.width, color='red')
        # set up the lables for the bars
        #self.ax.set_xticks(self.locs+0.5)
        #self.ax.set_xticklabels(['Red', 'Green', 'Black', 'Orange', 'Yellow'])

        # set the limit for the x and y        
        self.min_x, self.max_x, max_y = self.viewport(data)
        print self.min_x, self.max_x, max_y
        
        self.ax.set_ylim(self.min_x, self.max_x)
        self.ax.set_xlim(0., max_y) #inv 1
        #self.ax.set_position([1, 0, -1, 1])        
        #self.ax_intraday.invert_xaxis()
        
        
        # the left plot (Intraday)
        self.prices = [(self.data['bid_price'][0][0]+ self.data['ask_price'][0][0]) / 2]         
        self.lines, = self.ax_intraday.step(self.data.date[0], self.prices, linewidth = 2, color = 'gray')
                
        # the volummes plot (bottom right)                
        self.volumes = [self.data['volume'][0]]
        self.max_volume = 100
        self.trade_dates = [self.data.date[0]]
        self.zeros_base = [0]  
        self.stemvalues = [0]
        self.stemdates = [self.data.date[0]]      
        self.markerline, self.stemlines, self.baseline = self.ax_volume.stem(self.trade_dates, self.volumes)
        plt.setp(self.markerline, 'markerfacecolor', 'g')
        plt.setp(self.stemlines, 'color', 'g')
          
        # draw the canvas
        self.fig.canvas.draw()

        # start the timer        
        self.timer = self.startTimer(timer_length)

    def tick_size(self, data):
        min = 1e6
        for i in range(len(data['bid_price'])):
            for j in range(3):
                diff = data['bid_price'][i][j] - data['bid_price'][i][j+1]
                if (min > diff) & (diff > 0): min = diff
            for j in range(1, 4):
                diff = data['ask_price'][i][j+1] - data['ask_price'][i][j]
                if (min > diff) & (diff > 0): min = diff
        print min
        return min

    def viewport(self, data):
        min_x = 1e6
        max_x = 0
        index_extrem = 0
        
        max_y = 0
        for i in range(len(data['bid_price'])):
            if (min_x > data['bid_price'][i][index_extrem]) & (data['bid_price'][i][index_extrem] > 0): min_x = data['bid_price'][i][index_extrem]
            if (max_x < data['ask_price'][i][index_extrem]) & (data['ask_price'][i][index_extrem] < 10000000): max_x = data['ask_price'][i][index_extrem]
            for j in range(5):
                if max_y < data['bid_size'][i][j]: max_y = data['bid_size'][i][j]
                if max_y < data['ask_size'][i][j]: max_y = data['ask_size'][i][j]
        
        return min_x, max_x, max_y        
                
         

    def timerEvent(self, evt):        
        if self.counter >= len(self.data.date)-1: 
            self.killTimer(self.timer)
            return 
              
        # update viewport in real time        
        max_y = 1;
        for j in range(5):
                if max_y < self.data['bid_size'][self.counter][j]: max_y = self.data['bid_size'][self.counter][j]
                if max_y < self.data['ask_size'][self.counter][j]: max_y = self.data['ask_size'][self.counter][j]
        
        self.ax.set_xlim(0., max_y + max_y / 3)
        #self.ax.set_xlim(-max_y - max_y / 3, 0.)
        self.ax.set_ylim(self.min_x, self.max_x)
        
        #volume_ticks = [-max_y - max_y / 3, -max_y, -max_y + max_y / 3, -max_y + 2 * max_y / 3, 0]
        #self.ax.set_xticks(volume_ticks)
        #self.ax.xlabel('Volume')
        #self.ax.ylabel('Price')
        
        # update the height of the bars, one liner is easier
        [bar.set_width(self.data['bid_size'][self.counter][i]) for i,bar in enumerate(self.bars_bid)]
        [bar.set_y(self.data['bid_price'][self.counter][i]) for i,bar in enumerate(self.bars_bid)]
        
        [bar.set_width(self.data['ask_size'][self.counter][i]) for i,bar in enumerate(self.bars_ask)]
        [bar.set_y(self.data['ask_price'][self.counter][i]) for i,bar in enumerate(self.bars_ask)]
        self.ax.invert_xaxis()
        # update intraday price        
        self.prices.append((self.data['bid_price'][self.counter][0] + self.data['ask_price'][self.counter][0]) / 2)        
        self.ax_intraday.set_xlim(self.data.date[0], self.data.date[self.counter+1])
        self.ax_intraday.invert_xaxis()
        
        # set up the labels for the time axis
        if self.data.date[self.counter+1] == self.data.date[0]: # apply correction            
            some_microseconds = timedelta(days = 0, seconds = 0, microseconds = 10)
            self.data.date[self.counter+1] = self.data.date[self.counter+1] + some_microseconds
        
        start = timedelta(hours = self.data.date[0].hour, minutes = self.data.date[0].minute, seconds = self.data.date[0].second)
        end = timedelta(hours = self.data.date[self.counter+1].hour, minutes = self.data.date[self.counter+1].minute, seconds = self.data.date[self.counter+1].second)
        halfway = datetime(self.data.date[0].year, self.data.date[0].month, self.data.date[0].day) + (start + end) / 2        
        
        date_ticks = [self.data.date[self.counter+1],                      
                      halfway,                        
                      self.data.date[0]]
        
        self.ax_intraday.set_xticks(date_ticks)        
        self.ax_intraday.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        self.lines.set_data([self.data.date[self.counter+1-i] for i in range(len(self.prices))], [self.prices[len(self.prices)-1-i] for i in range(len(self.prices))])
        
        # set up the volume axes to plot
        # First separate the relevant (non-zero) volumes
        
        currentVolume = self.data['volume'][self.counter]
        currentDate = self.data.date[self.counter]
        if currentVolume > 0:
            self.volumes.append(currentVolume)
            self.trade_dates.append(currentDate)
            self.zeros_base.append(0)
            
            self.stemdates.append(currentDate)
            self.stemdates.append(currentDate)
            self.stemdates.append(currentDate)
            self.stemvalues.append(0)
            self.stemvalues.append(currentVolume)
            self.stemvalues.append(0)
            
            
        self.ax_volume.set_xlim(self.data.date[0], self.data.date[self.counter+1])   
        if self.max_volume < currentVolume: self.max_volume = currentVolume        
        volume_ticks = [self.max_volume, self.max_volume / 2, 0]
        
        self.ax_volume.set_yticks(volume_ticks)
        self.ax_volume.set_ylim(0., self.max_volume)
        self.markerline.set_data([self.trade_dates[len(self.trade_dates) - 1 - i] for i in range(len(self.trade_dates))], [self.volumes[len(self.trade_dates) - 1 - i] for i in range(len(self.trade_dates))])
        self.stemlines[0].set_data([self.stemdates[len(self.stemdates) - 1 - i] for i in range(len(self.stemdates))], [self.stemvalues[len(self.stemdates) - 1 - i] for i in range(len(self.stemdates))])
        self.baseline.set_data([self.zeros_base[len(self.trade_dates) - 1 - i] for i in range(len(self.trade_dates))], [self.volumes[len(self.trade_dates) - 1 - i] for i in range(len(self.trade_dates))])
        
        self.ax_volume.invert_xaxis()
        
        self.ax_volume.set_xticks(date_ticks)        
        self.ax_volume.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        # force the redraw of the canvas
        self.fig.canvas.draw()

        # update the data row counter
        self.counter += 1
        #print self.counter

if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    w = Monitor()
    w.setWindowTitle("Convergence")
    w.show()
    sys.exit(app.exec_())