package com.evn.lake.app;

import com.evn.lake.utils.ConfigUtils;

import java.io.IOException;

import static com.evn.lake.utils.RawIceberg.genAllDataRaw;

public class KinhDoanhTest {
    public static void main(String[] args) throws IOException {
        genAllDataRaw(ConfigUtils.EtlKinhDoanh.rawPath);
    }
}
