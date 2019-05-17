import os

for dim in ["date", "country", "product", "state", "flow", "transport"]:
        cmd = "bamboo-cli --folder . --entry dim_pipeline.run_dim --source-connector='brazil-ncm' --dim='{}'".format(dim)
        os.system(cmd)

for flow in ["EXP", "IMP"]:
    for year in [str(x) for x in range(1997,2020)]:
        cmd = "bamboo-cli --folder . --entry ncm_pipeline.run_brazil_ncm --source-connector='brazil-ncm' --year='{}' --flow='{}'".format(year, flow)
        os.system(cmd)