import { Ntp } from "../domain/generatedRpc/generatedClasses";

export const isNtpEqual = (ntp1: Ntp, ntp2: Ntp): boolean => {
  return (
    ntp1.topic === ntp2.topic &&
    ntp1.namespace === ntp2.namespace &&
    ntp1.partition === ntp2.partition
  );
};
