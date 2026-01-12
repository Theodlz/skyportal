export const ztf_crossmatch_fields = {
  name: "cross_matches",
  type: {
    type: "array",
    items: {
      type: "record",
      name: "CrossMatch",
      fields: [
        {
          name: "AllWISE",
          type: [
            "null",
            {
              type: "record",
              name: "AllWISEMatch",
              fields: [
                {
                  name: "_id",
                  type: "double",
                },
                {
                  name: "w1mpro",
                  type: "double",
                },
                {
                  name: "w1sigmpro",
                  type: "double",
                },
                {
                  name: "w2mpro",
                  type: "double",
                },
                {
                  name: "w2sigmpro",
                  type: "double",
                },
                {
                  name: "w3mpro",
                  type: "double",
                },
                {
                  name: "w3sigmpro",
                  type: "double",
                },
                {
                  name: "w4mpro",
                  type: "double",
                },
                {
                  name: "w4sigmpro",
                  type: "double",
                },
                {
                  name: "ph_qual",
                  type: "string",
                },
                {
                  name: "coordinates",
                  type: {
                    type: "record",
                    name: "WiseCoordinates",
                    fields: [
                      {
                        name: "radec_str",
                        type: {
                          type: "array",
                          items: "double",
                        },
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
        {
          name: "CLU_20190625",
          type: [
            "null",
            {
              type: "record",
              name: "CLUEntry",
              fields: [
                {
                  name: "_id",
                  type: "double",
                },
                {
                  name: "name",
                  type: "string",
                },
                {
                  name: "ra",
                  type: "double",
                },
                {
                  name: "dec",
                  type: "double",
                },
                {
                  name: "z",
                  type: "double",
                },
                {
                  name: "a",
                  type: "double",
                },
                {
                  name: "b2a",
                  type: "double",
                },
                {
                  name: "pa",
                  type: "double",
                },
                {
                  name: "sfr_ha",
                  type: "double",
                },
                {
                  name: "sfr_fuv",
                  type: "double",
                },
                {
                  name: "mstar",
                  type: "double",
                },
                {
                  name: "coordinates",
                  type: {
                    type: "record",
                    name: "CLUCoordinates",
                    fields: [
                      {
                        name: "radec_str",
                        type: {
                          type: "array",
                          items: "double",
                        },
                      },
                      {
                        name: "distance_arcsec",
                        type: "double",
                      },
                      {
                        name: "distance_kpc",
                        type: "double",
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
        {
          name: "NED_BetaV3",
          type: [
            "null",
            {
              type: "record",
              name: "NED_BetaV3Match",
              fields: [
                {
                  name: "_id",
                  type: "double",
                },
                {
                  name: "objname",
                  type: "string",
                },
                {
                  name: "ra",
                  type: "double",
                },
                {
                  name: "dec",
                  type: "double",
                },
                {
                  name: "objtype",
                  type: "double",
                },
                {
                  name: "z",
                  type: "double",
                },
                {
                  name: "z_unc",
                  type: "double",
                },
                {
                  name: "z_tech",
                  type: "double",
                },
                {
                  name: "z_qual",
                  type: "double",
                },
                {
                  name: "DistMpc",
                  type: "string",
                },
                {
                  name: "DistMpc_unc",
                  type: "float",
                },
                {
                  name: "ebv",
                  type: "float",
                },
                {
                  name: "m_Ks",
                  type: "float",
                },
                {
                  name: "m_Ks_unc",
                  type: "float",
                },
                {
                  name: "tMASSphot",
                  type: "string",
                },
                {
                  name: "coordinates",
                  type: {
                    type: "record",
                    name: "Coordinates",
                    fields: [
                      {
                        name: "radec_str",
                        type: {
                          type: "array",
                          items: "string",
                        },
                      },
                      {
                        name: "distance_arcsec",
                        type: "double",
                      },
                      {
                        name: "distance_kpc",
                        type: "double",
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
        {
          name: "Gaia_EDR3",
          type: [
            "null",
            {
              type: "record",
              name: "Gaia_EDR3Match",
              fields: [
                {
                  name: "_id",
                  type: "double",
                },
                {
                  name: "parallax",
                  type: "double",
                },
                {
                  name: "parallax_error",
                  type: "double",
                },
                {
                  name: "phot_g_mean_mag",
                  type: "double",
                },
                {
                  name: "phot_bp_mean_mag",
                  type: "double",
                },
                {
                  name: "phot_rp_mean_mag",
                  type: "double",
                },
                {
                  name: "coordinates",
                  type: {
                    type: "record",
                    name: "Coordinates",
                    fields: [
                      {
                        name: "radec_str",
                        type: {
                          type: "array",
                          items: "string",
                        },
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
      ],
    },
  },
};

export const lsst_crossmatch_fields = {
  name: "cross_matches",
  type: {
    type: "array",
    items: {
      type: "record",
      name: "CrossMatch",
      fields: [
        {
          name: "NED",
          type: [
            "null",
            {
              type: "record",
              name: "NEDMatch",
              fields: [
                {
                  name: "_id",
                  type: "double",
                },
                {
                  name: "objname",
                  type: "string",
                },
                {
                  name: "ra",
                  type: "double",
                },
                {
                  name: "dec",
                  type: "double",
                },
                {
                  name: "objtype",
                  type: "string",
                },
                {
                  name: "z",
                  type: "double",
                },
                {
                  name: "z_unc",
                  type: "double",
                },
                {
                  name: "z_tech",
                  type: "double",
                },
                {
                  name: "z_qual",
                  type: "double",
                },
                {
                  name: "DistMpc",
                  type: "string",
                },
                {
                  name: "DistMpc_unc",
                  type: "float",
                },
                {
                  name: "ebv",
                  type: "float",
                },
                {
                  name: "m_Ks",
                  type: "float",
                },
                {
                  name: "m_Ks_unc",
                  type: "float",
                },
                {
                  name: "tMASSphot",
                  type: "string",
                },
                {
                  name: "coordinates",
                  type: {
                    type: "record",
                    name: "Coordinates",
                    fields: [
                      {
                        name: "radec_str",
                        type: {
                          type: "array",
                          items: "string",
                        },
                      },
                      {
                        name: "distance_arcsec",
                        type: "double",
                      },
                      {
                        name: "distance_kpc",
                        type: "double",
                      },
                    ],
                  },
                },
              ],
            },
          ],
          default: null,
        },
      ],
    },
  },
};
