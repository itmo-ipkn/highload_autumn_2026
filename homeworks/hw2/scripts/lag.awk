# kafka-consumer-groups --describe; a missing/invalid partition is not zero lag.
$1 == "GROUP" || $0 ~ /(^|[[:space:]])GROUP([[:space:]]|$)/ {
    for (i = 1; i <= NF; i++) col[$i] = i
    next
}
col["GROUP"] && col["TOPIC"] && col["PARTITION"] && col["LAG"] && $(col["GROUP"]) == group && $(col["TOPIC"]) == topic {
    p = $(col["PARTITION"]); lag = $(col["LAG"])
    if (p !~ /^[012]$/ || seen[p]++ || lag !~ /^[0-9]+$/) invalid = 1
    total += lag
}
END {
    if (!invalid && seen[0] && seen[1] && seen[2]) print total + 0
    else print "unknown"
}
