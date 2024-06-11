class Battery():
    def __init__(self, capacity=1000*120*6, max_power=1000):
        self.max_capacity = capacity
        self.capacity = self.max_capacity
        self.max_power = max_power
    
    def charge(self, power):
        if power >= self.max_power:
            power = self.max_power
        if self.capacity + power >= self.max_capacity:
            power = self.max_capacity - self.capacity
            self.capacity = self.max_capacity
        else:
            self.capacity += power
        return power
    
    def discharge(self, power):
        if power >= self.max_power:
            power = self.max_power
        if self.capacity - power <= 0:
            power = self.capacity
            self.capacity = 0
        else:
            self.capacity -= power
        return power