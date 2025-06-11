package syscnpg

import (
	"errors"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"pgdistbench/pkg/client/systems"
	"pgdistbench/pkg/k8util"
)

type EndpointInfo struct {
	Host     string
	Port     int
	User     string
	Password string
}

func CreateClusterConnEnvVars(secretKey string) []corev1.EnvVar {
	return []corev1.EnvVar{
		k8util.SecretKeyEnvVar(systems.EnvVarPGHost, secretKey, "host"),
		k8util.SecretKeyEnvVar(systems.EnvVarPGPort, secretKey, "port"),
		k8util.SecretKeyEnvVar(systems.EnvVarPGUser, secretKey, "user"),
		k8util.SecretKeyEnvVar(systems.EnvVarPGPass, secretKey, "password"),
	}
}

func EndpointFromSecret(secret *corev1.Secret) (info EndpointInfo, err error) {
	if len(secret.Data) == 0 {
		return info, errors.New("empty secret")
	}

	host, ok := secret.Data["host"]
	if !ok {
		return info, errors.New("missing host")
	}
	info.Host = string(host)

	info.Port = 5432
	if portRaw, ok := secret.Data["port"]; ok {
		portStr := string(portRaw)
		info.Port, err = strconv.Atoi(portStr)
		if err != nil {
			return info, fmt.Errorf("parse port: %w", err)
		}
	}

	info.User = "postgres"
	info.Password = ""
	if user, ok := secret.Data["user"]; ok {
		info.User = string(user)
	}
	if pass, ok := secret.Data["password"]; ok {
		info.Password = string(pass)
	}

	return info, nil
}
