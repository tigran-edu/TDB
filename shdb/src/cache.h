#pragma once

#include <vector>
#include <stdexcept>
#include <unordered_map>
#include <iostream>

namespace shdb
{

template <class Key, class Value>
class ClockCache
{
public:
    explicit ClockCache(std::vector<Value> free_values) : myclock(free_values)
    {
    }

    std::pair<bool, Value> find(const Key & key)
    {
        if (!myclock.values.contains(key)) {
            return {false, 0};
        }

        auto index = myclock.values[key];
        myclock.ring[index].amount = std::min(5, myclock.ring[index].amount + 1);
        return {true, myclock.ring[index].value};
    }

    Value put(const Key & key)
    {
        while (true) {
            if (myclock.ring[time].amount > 0) 
            {
                    myclock.ring[time].amount -= 1;
            }
            else if (myclock.ring[time].amount == 0 && !myclock.ring[time].lock) 
            {
                for (auto &  [key, val] : myclock.values) {
                    if (val == time) {
                        myclock.values.erase(key);
                        break;
                    }
                }
                myclock.values[key] = time;
                myclock.ring[time].amount = 1;
                return myclock.ring[time].value;
            }

            time++;
            time %= myclock.ring.size();
        }
    }

    void lock(const Key & key)
    {
        auto index = myclock.values[key];
        myclock.ring[index].lock = true;
    }

    void unlock(const Key & key)
    {
        auto index = myclock.values[key];
        myclock.ring[index].lock = false;
    }

private:
    struct Cell {
        Cell(Value value)
        {
            this->value = value;
        }
        Value value;
        int amount = 0;
        bool lock = false;
    };

    struct MyClock {
        MyClock(std::vector<Value> & free_values) {
            for (auto & value : free_values) {
                ring.push_back({Cell(value)});
            }
        }
        std::vector<Cell> ring;
        std::unordered_map<Key, int> values;
    };

   MyClock myclock;
   uint64_t time = 0;
};

}