# PDS archive reachability

Last checked: 2026-09-10 11:51 UTC

| host | indexes | status |
| --- | --- | --- |
| HiRISE (LPL) | 3 | up |
| PDS Geosciences (WUSTL) | 22 | **15 failing** |
| PDS Rings (SETI) | 42 | up |
| LROC (ASU) | 1 | up |
| PDS Imaging (JPL) | 8 | up |
| SBN (PSI) | 1 | up |

## Failing indexes

| index key | status | url |
| --- | --- | --- |
| `lro.lola.edr` | **ReadTimeout** | http://pds-geosciences.wustl.edu/lro/lro-l-lola-2-edr-v1/lrolol_0xxx/index/edrindex.lbl |
| `lro.lola.rdr` | **ReadTimeout** | http://pds-geosciences.wustl.edu/lro/lro-l-lola-3-rdr-v1/lrolol_1xxx/index/rdrindex.lbl |
| `msl.apxs.edr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/msl/msl-m-apxs-2-edr-v1/mslapx_0xxx/index/index.lbl |
| `msl.apxs.rdr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/msl/msl-m-apxs-4_5-rdr-v1/mslapx_1xxx/index/index.lbl |
| `msl.ccam.edr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/msl/msl-m-chemcam-libs-2-edr-v1/mslccm_0xxx/index/index.lbl |
| `msl.ccam.libs_rdr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/msl/msl-m-chemcam-libs-4_5-rdr-v1/mslccm_1xxx/index/libsindex.lbl |
| `msl.ccam.rmi_rdr` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-chemcam-libs-4_5-rdr-v1/mslccm_1xxx/index/rmiindex.lbl |
| `msl.cmn.edr` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-chemin-2-edr-v1/mslcmn_0xxx/index/index.lbl |
| `msl.cmn.rdr` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-chemin-4-rdr-v1/mslcmn_1xxx/index/index.lbl |
| `msl.sam.l0` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-sam-2-rdr-l0-v1/mslsam_1xxx/index/l0_index.lbl |
| `msl.sam.l1a` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-sam-2-rdr-l0-v1/mslsam_1xxx/index/l1a_index.lbl |
| `msl.sam.l1b` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-sam-2-rdr-l0-v1/mslsam_1xxx/index/l1b_index.lbl |
| `msl.sam.l2` | **503** | https://pds-geosciences.wustl.edu/msl/msl-m-sam-2-rdr-l0-v1/mslsam_1xxx/index/l2_index.lbl |
| `phoenix.meca.edr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/phx/phx-m-meca-2-niedr-v1/phxmec_0xxx/index/index.lbl |
| `phoenix.meca.rdr` | **ReadTimeout** | https://pds-geosciences.wustl.edu/phx/phx-m-meca-4-nirdr-v1/phxmec_1xxx/index/index.lbl |

62/77 index URLs reachable across 6 hosts.
