using ImcFamosFile;
using Nexus.DataModel;
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Nexus.Sources
{
    internal static class FamosUtilities
    {
        #region Methods

        public static NexusDataType GetNexusDataTypeFromFamosDataType(FamosFileDataType dataType)
        {
            return dataType switch
            {
                FamosFileDataType.UInt8 => NexusDataType.UINT8,
                FamosFileDataType.Int8 => NexusDataType.INT8,
                FamosFileDataType.UInt16 => NexusDataType.UINT16,
                FamosFileDataType.Int16 => NexusDataType.INT16,
                FamosFileDataType.UInt32 => NexusDataType.UINT32,
                FamosFileDataType.Int32 => NexusDataType.INT32,
                FamosFileDataType.Float32 => NexusDataType.FLOAT32,
                FamosFileDataType.Float64 => NexusDataType.FLOAT64,
                FamosFileDataType.ImcDevicesTransitionalRecording => 0,
                FamosFileDataType.AsciiTimeStamp => 0,
                FamosFileDataType.Digital16Bit => 0,
                FamosFileDataType.UInt48 => 0,
                _ => 0
            };
        }

        public static Type GetTypeFromNexusDataType(NexusDataType dataType)
        {
            return dataType switch
            {
                NexusDataType.UINT8 => typeof(Byte),
                NexusDataType.INT8 => typeof(SByte),
                NexusDataType.UINT16 => typeof(UInt16),
                NexusDataType.INT16 => typeof(Int16),
                NexusDataType.UINT32 => typeof(UInt32),
                NexusDataType.INT32 => typeof(Int32),
                NexusDataType.UINT64 => typeof(UInt64),
                NexusDataType.INT64 => typeof(Int64),
                NexusDataType.FLOAT32 => typeof(Single),
                NexusDataType.FLOAT64 => typeof(Double),
                _ => throw new NotSupportedException($"The specified data type '{dataType}' is not supported.")
            };
        }

        public static object InvokeGenericMethod<T>(T instance, string methodName, BindingFlags bindingFlags, Type genericType, object[] parameters)
        {
            return FamosUtilities.InvokeGenericMethod(typeof(T), instance, methodName, bindingFlags, genericType, parameters);
        }

        public static object InvokeGenericMethod(Type methodParent, object instance, string methodName, BindingFlags bindingFlags, Type genericType, object[] parameters)
        {
            var methodInfo = methodParent
                .GetMethods(bindingFlags)
                .Where(methodInfo => methodInfo.IsGenericMethod && methodInfo.Name == methodName)
                .First();

            var genericMethodInfo = methodInfo.MakeGenericMethod(genericType);
            var result = genericMethodInfo.Invoke(instance, parameters);

            if (result is null)
                throw new Exception("result is null");

            return result;
        }

        public static unsafe double[] ToDouble<T>(Span<T> dataset) where T : unmanaged
        {
            var doubleData = new double[dataset.Length];

            fixed (T* dataPtr = dataset)
            {
                FamosUtilities.InternalToDouble(dataPtr, doubleData);
            }

            return doubleData;
        }

        internal static unsafe void InternalToDouble<T>(T* dataPtr, double[] doubleData) where T : unmanaged
        {
            Parallel.For(0, doubleData.Length, i =>
            {
                doubleData[i] = GenericToDouble<T>.ToDouble(dataPtr[i]);
            });
        }

        public static class GenericToDouble<T>
        {
            private static Func<T, double> _to_double_function = GenericToDouble<T>.EmitToDoubleConverter();

            private static Func<T, double> EmitToDoubleConverter()
            {
                var method = new DynamicMethod(string.Empty, typeof(double), new Type[] { typeof(T) });
                var ilGenerator = method.GetILGenerator();

                ilGenerator.Emit(OpCodes.Ldarg_0);

                if (typeof(T) != typeof(double))
                    ilGenerator.Emit(OpCodes.Conv_R8);

                ilGenerator.Emit(OpCodes.Ret);

                return (Func<T, double>)method.CreateDelegate(typeof(Func<T, double>));
            }

            public static double ToDouble(T value)
            {
                return _to_double_function(value);
            }
        }

        #endregion
    }
}
