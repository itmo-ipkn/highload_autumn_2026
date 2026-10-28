math.randomseed(os.time())

local valid_ids = { "1", "2", "3", "4", "5", "10", "42" }

function request()
    local r = math.random(100)
    local err_roll = math.random(100)

    -- 1. POST /items
    if r <= 35 then
        local headers = { ["Content-Type"] = "application/json" }
        
        -- 5% шанс получить 500
        if err_roll <= 5 then
            headers["X-Simulate-Error"] = "500"
        end
        
        local body = '{"name": "item-' .. math.random(10000) .. '"}'
        return wrk.format("POST", "/items", headers, body)

    -- 2. GET /items/{id}
    elseif r <= 75 then
        local path = "/items/"
        
        if err_roll <= 5 then
            path = path .. tostring(math.random(1000000, 9999999))
        elseif err_roll <= 10 then
            path = path .. valid_ids[math.random(#valid_ids)]
            return wrk.format("GET", path, {["X-Simulate-Error"] = "500"}, nil)
        else
            -- Успешный GET
            path = path .. valid_ids[math.random(#valid_ids)]
        end
        return wrk.format("GET", path, {}, nil)

    -- 3. PUT /items/{id}
    else
        local headers = { ["Content-Type"] = "application/json" }
        local path = "/items/"
        
        if err_roll <= 5 then
            path = path .. tostring(math.random(1000000, 9999999))
        else
            path = path .. valid_ids[math.random(#valid_ids)]
        end
        
        local body = '{"name": "updated-item"}'
        return wrk.format("PUT", path, headers, body)
    end
end