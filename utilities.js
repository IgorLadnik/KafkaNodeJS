function getIp() {
    var address,
        ifaces = require('os').networkInterfaces();
    for (var dev in ifaces) {
        ifaces[dev].filter(details => {
            address = details.family === 'IPv4' && details.internal === false
                ? details.address
                : undefined;
        });

        if (address !== undefined)
            break;
    }

    return address;
}

// EXPORT
module.exports.getIp = getIp;