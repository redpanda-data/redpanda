This test is related to two issues. The first is not directly tested, but is
generally a problem that existed with the Sarama client interacting with
consumer groups in redpanda:

   0e112352d1d77d559f65fc6f614d3558e30c4dde

The second issue is directly tested by this patch and is related to an
off-by-one error when consuming with oldest offset setting.

   c4bd1c58e9cb1ccc41a8a234da7541997fb7184b
