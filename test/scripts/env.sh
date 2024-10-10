# https://github.com/emqx/emqx-builder
export EMQX_BUILDER_VSN=5.3-13
export OTP_VSN=26.2.5.2
export EMQX_OTP_VSN=${OTP_VSN}-1
export ELIXIR_VSN=1.15.7
export UBUNTU_VSN=22.04
export EMQX_BUILDER=ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VSN}:${ELIXIR_VSN}-${EMQX_OTP_VSN}-ubuntu${UBUNTU_VSN}
export EMQX_IMAGE=emqx/emqx-enterprise:5.8.0
